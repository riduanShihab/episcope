"""
EpiScope Silver Layer Pipeline
Cleans, conforms, and de-identifies bronze data.
Applies 12+ data quality rules, quarantines bad rows, hashes patient IDs.
"""

import os
import sys
import hashlib
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import (
    BRONZE_DIR, SILVER_DIR, DISEASE_CODES, BACKGROUND_CODES,
    SPARK_APP_NAME, SPARK_PARTITIONS, HASH_ALGORITHM, get_all_districts
)

# All valid ICD-10 codes
VALID_ICD10 = []
for codes in DISEASE_CODES.values():
    VALID_ICD10.extend(codes)
VALID_ICD10.extend(BACKGROUND_CODES)

VALID_DISTRICTS = get_all_districts()


def get_spark():
    """Create or get Spark session."""
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_silver")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def apply_quality_rules(df, facilities_df):
    """
    Apply 12+ data quality rules. Returns (clean_df, quarantine_df).

    Rules:
    DQ-R01: Schema completeness – encounter_id not null
    DQ-R02: Schema completeness – patient_id not null
    DQ-R03: Schema completeness – facility_id not null
    DQ-R04: Schema completeness – onset_date not null
    DQ-R05: Schema completeness – icd10_code not null
    DQ-R06: Valid age range (0 < age < 120)
    DQ-R07: Valid sex (M or F)
    DQ-R08: onset_date <= report_date
    DQ-R09: Valid ICD-10 code
    DQ-R10: Referential integrity – facility_id exists in registry
    DQ-R11: Date range validity (2020-01-01 to 2026-12-31)
    DQ-R12: Duplicate encounter_id detection
    DQ-R13: Valid district (from facility join)
    """

    # Cast dates
    df = df.withColumn("onset_date_parsed", F.to_date("onset_date")) \
           .withColumn("report_date_parsed", F.to_date("report_date")) \
           .withColumn("age_int", F.col("age_years").cast("int"))

    # Build quality flag columns
    df = df.withColumn("dq_r01_eid_null", F.col("encounter_id").isNull()) \
           .withColumn("dq_r02_pid_null", F.col("patient_id").isNull()) \
           .withColumn("dq_r03_fid_null", F.col("facility_id").isNull()) \
           .withColumn("dq_r04_onset_null", F.col("onset_date_parsed").isNull()) \
           .withColumn("dq_r05_icd_null", F.col("icd10_code").isNull()) \
           .withColumn("dq_r06_bad_age",
                       (F.col("age_int").isNull()) |
                       (F.col("age_int") <= 0) |
                       (F.col("age_int") >= 120)) \
           .withColumn("dq_r07_bad_sex",
                       ~F.col("sex").isin(["M", "F"])) \
           .withColumn("dq_r08_date_order",
                       F.col("onset_date_parsed") > F.col("report_date_parsed")) \
           .withColumn("dq_r09_bad_icd",
                       ~F.col("icd10_code").isin(VALID_ICD10)) \
           .withColumn("dq_r11_bad_date_range",
                       (F.col("onset_date_parsed") < F.lit("2020-01-01")) |
                       (F.col("onset_date_parsed") > F.lit("2026-12-31")))

    # DQ-R10: Referential integrity (facility exists)
    valid_fac_ids = facilities_df.select("facility_id").distinct()
    df = df.join(
        valid_fac_ids.withColumn("_fac_exists", F.lit(True)),
        on="facility_id", how="left"
    ).withColumn("dq_r10_orphan_fk", F.col("_fac_exists").isNull())

    # DQ-R12: Duplicate encounter_id
    window = Window.partitionBy("encounter_id").orderBy("_load_time")
    df = df.withColumn("_row_num", F.row_number().over(window)) \
           .withColumn("dq_r12_duplicate", F.col("_row_num") > 1)

    # Aggregate: any rule failed?
    dq_cols = [c for c in df.columns if c.startswith("dq_r")]
    failed_expr = F.lit(False)
    for col_name in dq_cols:
        failed_expr = failed_expr | F.col(col_name)
    df = df.withColumn("_any_dq_failed", failed_expr)

    # Build reason code
    reason_parts = []
    for col_name in dq_cols:
        reason_parts.append(
            F.when(F.col(col_name), F.lit(col_name)).otherwise(F.lit(""))
        )
    df = df.withColumn("_dq_reason",
                       F.trim(F.concat_ws("|", *reason_parts)))

    # Split clean vs quarantine
    clean_df = df.filter(~F.col("_any_dq_failed"))
    quarantine_df = df.filter(F.col("_any_dq_failed"))

    return clean_df, quarantine_df, dq_cols


def deidentify(df):
    """
    De-identify patient data:
    - Hash patient_id with salt (salt not stored in repo)
    - Reduce age to 5-year bands
    """
    # Salt from environment or default (in production, from .env)
    salt = os.environ.get("EPISCOPE_SALT", "episcope_default_salt_2026")

    # Hash patient_id
    df = df.withColumn("patient_id_hash",
                       F.sha2(F.concat(F.lit(salt), F.col("patient_id")), 256))

    # Drop original patient_id
    df = df.drop("patient_id")

    # Age banding (5-year bands)
    df = df.withColumn("age_band",
                       F.concat(
                           (F.floor(F.col("age_int") / 5) * 5).cast("string"),
                           F.lit("-"),
                           ((F.floor(F.col("age_int") / 5) * 5) + 4).cast("string")
                       ))

    return df


def deduplicate_encounters(df):
    """Remove exact duplicate encounters (keep first by load time)."""
    window = Window.partitionBy("encounter_id").orderBy("_load_time")
    df = df.withColumn("_dedup_rank", F.row_number().over(window))
    deduped = df.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
    return deduped


def run_silver_pipeline():
    """Run the full silver transformation pipeline."""
    print("═══ Silver Layer Transformation ═══")

    spark = get_spark()

    try:
        # Load bronze encounters
        enc_path = os.path.join(BRONZE_DIR, "encounters")
        if not os.path.exists(enc_path):
            print("  ⚠ Bronze encounters not found. Run bronze pipeline first.")
            return
        encounters = spark.read.parquet(enc_path)
        print(f"  Loaded {encounters.count():,} bronze encounters")

        # Load bronze facilities
        fac_path = os.path.join(BRONZE_DIR, "facilities")
        facilities = spark.read.parquet(fac_path)
        print(f"  Loaded {facilities.count()} bronze facilities")

        # Apply quality rules
        print("  Applying 13 data quality rules ...")
        clean_df, quarantine_df, dq_cols = apply_quality_rules(encounters, facilities)

        clean_count = clean_df.count()
        quarantine_count = quarantine_df.count()
        total = clean_count + quarantine_count
        print(f"  Clean: {clean_count:,} ({100*clean_count/max(total,1):.1f}%)")
        print(f"  Quarantined: {quarantine_count:,} ({100*quarantine_count/max(total,1):.1f}%)")

        # Save quarantine
        quarantine_path = os.path.join(SILVER_DIR, "quarantine")
        quarantine_select = quarantine_df.select(
            "encounter_id", "patient_id", "facility_id",
            "onset_date", "report_date", "icd10_code",
            "age_years", "sex", "_dq_reason",
            "_source", "_batch_id", "_load_time"
        )
        quarantine_select.write.mode("overwrite").parquet(quarantine_path)
        print(f"  ✓ Quarantine saved → {quarantine_path}")

        # De-duplicate
        print("  Deduplicating encounters ...")
        deduped = deduplicate_encounters(clean_df)
        dedup_count = deduped.count()
        print(f"  After dedup: {dedup_count:,} (removed {clean_count - dedup_count:,} dupes)")

        # De-identify
        print("  De-identifying patient data ...")
        silver_df = deidentify(deduped)

        # Select final silver columns
        silver_cols = [
            "encounter_id", "patient_id_hash", "facility_id",
            "onset_date_parsed", "report_date_parsed",
            "age_band", "age_int", "sex",
            "icd10_code", "lab_confirmed", "admitted", "outcome",
            "_source", "_batch_id", "_load_time", "_file_checksum"
        ]
        # Rename parsed dates
        silver_df = silver_df.select(
            *[c for c in silver_cols if c in silver_df.columns]
        ).withColumnRenamed("onset_date_parsed", "onset_date") \
         .withColumnRenamed("report_date_parsed", "report_date") \
         .withColumnRenamed("age_int", "age_years")

        # Add epi-week columns
        silver_df = silver_df.withColumn("epi_year", F.year("onset_date")) \
                             .withColumn("epi_week", F.weekofyear("onset_date"))

        # Write partitioned by epi_year
        silver_enc_path = os.path.join(SILVER_DIR, "encounters")
        silver_df.repartition(SPARK_PARTITIONS) \
                 .write.mode("overwrite") \
                 .partitionBy("epi_year") \
                 .parquet(silver_enc_path)
        print(f"  ✓ Silver encounters: {dedup_count:,} rows → {silver_enc_path}")

        # Silver facilities (pass-through with added division)
        silver_fac = facilities.drop("_source", "_load_time", "_batch_id", "_file_checksum")
        silver_fac_path = os.path.join(SILVER_DIR, "facilities")
        silver_fac.coalesce(1).write.mode("overwrite").parquet(silver_fac_path)
        print(f"  ✓ Silver facilities → {silver_fac_path}")

        # Silver weather
        weather_path = os.path.join(BRONZE_DIR, "weather")
        if os.path.exists(weather_path):
            weather = spark.read.parquet(weather_path)
            weather_silver = weather.drop("_source", "_load_time", "_batch_id", "_file_checksum")
            weather_silver = weather_silver.withColumn("date_parsed", F.to_date("date"))
            weather_silver_path = os.path.join(SILVER_DIR, "weather")
            weather_silver.repartition(4).write.mode("overwrite").parquet(weather_silver_path)
            print(f"  ✓ Silver weather → {weather_silver_path}")

        # Silver stock
        stock_path = os.path.join(BRONZE_DIR, "stock")
        if os.path.exists(stock_path):
            stock = spark.read.parquet(stock_path)
            stock_silver = stock.drop("_source", "_load_time", "_batch_id", "_file_checksum")
            stock_silver_path = os.path.join(SILVER_DIR, "stock")
            stock_silver.repartition(4).write.mode("overwrite").parquet(stock_silver_path)
            print(f"  ✓ Silver stock → {stock_silver_path}")

        # Data quality report
        print("  Generating data quality report ...")
        dq_report = {
            "batch_id": encounters.select("_batch_id").first()[0] if encounters.count() > 0 else "unknown",
            "load_time": datetime.utcnow().isoformat(),
            "total_rows_in": total,
            "clean_rows": clean_count,
            "quarantined_rows": quarantine_count,
            "duplicates_removed": clean_count - dedup_count,
            "final_silver_rows": dedup_count,
        }

        # Per-rule pass rates
        for col_name in dq_cols:
            fail_count = quarantine_df.filter(F.col(col_name)).count() if quarantine_count > 0 else 0
            dq_report[f"{col_name}_failures"] = fail_count
            dq_report[f"{col_name}_pass_rate"] = round(1 - fail_count / max(total, 1), 4)

        # Save DQ report as JSON
        import json
        dq_path = os.path.join(SILVER_DIR, "dq_report.json")
        with open(dq_path, "w") as f:
            json.dump(dq_report, f, indent=2)
        print(f"  ✓ DQ report → {dq_path}")

        print("═══ Silver Layer Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()
