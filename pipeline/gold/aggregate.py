"""
EpiScope Gold Layer Pipeline
Produces epi-week aggregate marts, feature tables, and analytics-ready datasets.
Enforces small-cell suppression (counts 1-4 → null).
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import (
    SILVER_DIR, GOLD_DIR, DISEASE_CODES, SMALL_CELL_THRESHOLD,
    SPARK_APP_NAME, SPARK_PARTITIONS, DISTRICT_POPULATIONS, get_all_districts
)

# Map ICD-10 codes to disease names
ICD10_TO_DISEASE = {}
for disease, codes in DISEASE_CODES.items():
    for code in codes:
        ICD10_TO_DISEASE[code] = disease


def get_spark():
    """Create or get Spark session."""
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_gold")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def suppress_small_cells(df, count_cols):
    """Replace counts 1-4 with null (small-cell suppression for privacy)."""
    for col_name in count_cols:
        df = df.withColumn(
            col_name,
            F.when(
                (F.col(col_name) >= 1) & (F.col(col_name) < SMALL_CELL_THRESHOLD),
                F.lit(None).cast("long")
            ).otherwise(F.col(col_name))
        )
    return df


def build_epiweek_mart(spark, encounters, facilities):
    """
    Build the core epi-week mart: weekly case counts by district and disease.
    Includes incidence per 100k, admissions, deaths.
    """
    # Map ICD-10 to disease
    icd_mapping = spark.createDataFrame(
        [(code, disease) for code, disease in ICD10_TO_DISEASE.items()],
        ["icd10_code", "disease"]
    )

    # Join encounters with disease mapping
    df = encounters.join(icd_mapping, on="icd10_code", how="inner")

    # Join with facilities to get district
    df = df.join(
        facilities.select("facility_id", "district", "division"),
        on="facility_id", how="inner"
    )

    # Aggregate by district, disease, epi_year, epi_week
    weekly = df.groupBy("district", "division", "disease", "epi_year", "epi_week").agg(
        F.count("*").alias("case_count"),
        F.sum(F.when(F.col("lab_confirmed") == "true", 1).otherwise(0)).alias("lab_confirmed_count"),
        F.sum(F.when(F.col("admitted") == "true", 1).otherwise(0)).alias("admissions"),
        F.sum(F.when(F.col("outcome") == "deceased", 1).otherwise(0)).alias("deaths"),
        F.avg("age_years").alias("mean_age"),
    )

    # Add population and incidence per 100k
    pop_data = [(d, p * 1000) for d, p in DISTRICT_POPULATIONS.items()]
    pop_df = spark.createDataFrame(pop_data, ["district", "population"])

    weekly = weekly.join(pop_df, on="district", how="left")
    weekly = weekly.withColumn(
        "incidence_per_100k",
        F.round(F.col("case_count") / F.col("population") * 100000 * 52, 2)  # annualised
    )

    # Small-cell suppression
    weekly = suppress_small_cells(weekly, ["case_count", "lab_confirmed_count", "admissions", "deaths"])

    return weekly


def build_weather_features(spark):
    """Build weekly weather features from silver weather data."""
    weather_path = os.path.join(SILVER_DIR, "weather")
    if not os.path.exists(weather_path):
        print("  ⚠ Silver weather not found, skipping weather features")
        return None

    weather = spark.read.parquet(weather_path)

    # Add epi-week
    weather = weather.withColumn("date_col", F.to_date("date")) \
                     .withColumn("epi_year", F.year("date_col")) \
                     .withColumn("epi_week", F.weekofyear("date_col"))

    # Weekly aggregation
    weekly_weather = weather.groupBy("district", "epi_year", "epi_week").agg(
        F.avg("temperature").alias("avg_temp"),
        F.sum("precipitation").alias("total_precip"),
        F.avg("humidity").alias("avg_humidity"),
        F.max("temperature").alias("max_temp"),
        F.min("temperature").alias("min_temp"),
    )

    # Lagged features (4-8 week lags for dengue climate drivers)
    window = Window.partitionBy("district").orderBy("epi_year", "epi_week")
    for lag in [4, 6, 8]:
        weekly_weather = weekly_weather \
            .withColumn(f"precip_lag{lag}w", F.lag("total_precip", lag).over(window)) \
            .withColumn(f"temp_lag{lag}w", F.lag("avg_temp", lag).over(window))

    return weekly_weather


def build_stock_summary(spark):
    """Build weekly stock summary from silver stock data."""
    stock_path = os.path.join(SILVER_DIR, "stock")
    if not os.path.exists(stock_path):
        print("  ⚠ Silver stock not found, skipping stock summary")
        return None

    stock = spark.read.parquet(stock_path)

    # Join with facilities to get district
    fac_path = os.path.join(SILVER_DIR, "facilities")
    facilities = spark.read.parquet(fac_path)
    stock = stock.join(facilities.select("facility_id", "district"), on="facility_id", how="inner")

    stock = stock.withColumn("snap_date", F.to_date("snapshot_date")) \
                 .withColumn("epi_year", F.year("snap_date")) \
                 .withColumn("epi_week", F.weekofyear("snap_date"))

    weekly_stock = stock.groupBy("district", "epi_year", "epi_week").agg(
        F.sum("iv_saline_units").alias("total_iv_saline"),
        F.sum("ns1_test_kits").alias("total_ns1_kits"),
        F.sum("ors_packets").alias("total_ors"),
        F.sum("antimalarials_doses").alias("total_antimalarials"),
        F.sum("occupied_beds").alias("total_occupied_beds"),
        F.sum("total_beds").alias("total_beds"),
    )

    weekly_stock = weekly_stock.withColumn(
        "bed_occupancy_rate",
        F.round(F.col("total_occupied_beds") / F.col("total_beds"), 3)
    )

    return weekly_stock


def build_dq_gold_table(spark):
    """Store DQ report as a gold table."""
    import json
    dq_path = os.path.join(SILVER_DIR, "dq_report.json")
    if os.path.exists(dq_path):
        with open(dq_path) as f:
            dq = json.load(f)
        rows = [(k, str(v)) for k, v in dq.items()]
        dq_df = spark.createDataFrame(rows, ["metric", "value"])
        dq_gold_path = os.path.join(GOLD_DIR, "dq_report")
        dq_df.coalesce(1).write.mode("overwrite").parquet(dq_gold_path)
        print(f"  ✓ Gold DQ report → {dq_gold_path}")


def run_gold_pipeline():
    """Run the full gold layer pipeline."""
    print("═══ Gold Layer Aggregation ═══")

    spark = get_spark()

    try:
        # Load silver data
        enc_path = os.path.join(SILVER_DIR, "encounters")
        fac_path = os.path.join(SILVER_DIR, "facilities")

        if not os.path.exists(enc_path):
            print("  ⚠ Silver encounters not found. Run silver pipeline first.")
            return

        encounters = spark.read.parquet(enc_path)
        facilities = spark.read.parquet(fac_path)
        print(f"  Loaded {encounters.count():,} silver encounters")

        # 1. Epi-week mart
        print("  Building epi-week mart ...")
        epiweek_mart = build_epiweek_mart(spark, encounters, facilities)
        epiweek_path = os.path.join(GOLD_DIR, "epiweek_mart")
        epiweek_mart.repartition(SPARK_PARTITIONS) \
                    .write.mode("overwrite") \
                    .partitionBy("epi_year") \
                    .parquet(epiweek_path)
        print(f"  ✓ Gold epi-week mart: {epiweek_mart.count():,} rows → {epiweek_path}")

        # 2. Weather features
        print("  Building weather features ...")
        weather_features = build_weather_features(spark)
        if weather_features is not None:
            weather_path = os.path.join(GOLD_DIR, "weather_features")
            weather_features.repartition(4).write.mode("overwrite").parquet(weather_path)
            print(f"  ✓ Gold weather features → {weather_path}")

        # 3. Stock summary
        print("  Building stock summary ...")
        stock_summary = build_stock_summary(spark)
        if stock_summary is not None:
            stock_path = os.path.join(GOLD_DIR, "stock_summary")
            stock_summary.repartition(4).write.mode("overwrite").parquet(stock_path)
            print(f"  ✓ Gold stock summary → {stock_path}")

        # 4. DQ gold table
        build_dq_gold_table(spark)

        # 5. Feature table (epi-week + weather joined)
        print("  Building feature table ...")
        feature_table = epiweek_mart
        if weather_features is not None:
            feature_table = feature_table.join(
                weather_features,
                on=["district", "epi_year", "epi_week"],
                how="left"
            )
        if stock_summary is not None:
            feature_table = feature_table.join(
                stock_summary,
                on=["district", "epi_year", "epi_week"],
                how="left"
            )

        feature_path = os.path.join(GOLD_DIR, "feature_table")
        feature_table.repartition(SPARK_PARTITIONS) \
                     .write.mode("overwrite") \
                     .partitionBy("epi_year") \
                     .parquet(feature_path)
        print(f"  ✓ Gold feature table → {feature_path}")

        print("═══ Gold Layer Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_pipeline()
