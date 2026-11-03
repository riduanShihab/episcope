"""
EpiScope Bronze Layer Pipeline
Ingests raw data sources into the bronze layer without modification,
adding ingestion metadata (source, load_time, batch_id, file_checksum).
"""

import os
import sys
import hashlib
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    BooleanType, DateType, TimestampType
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import RAW_DIR, BRONZE_DIR, SPARK_APP_NAME, SPARK_PARTITIONS


def get_file_checksum(filepath):
    """Compute SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_spark():
    """Create or get Spark session."""
    return (SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())


def ingest_encounters(spark, batch_id):
    """Ingest encounter CSV into bronze Parquet."""
    filepath = os.path.join(RAW_DIR, "encounters.csv")
    if not os.path.exists(filepath):
        print(f"  ⚠ encounters.csv not found, skipping")
        return

    checksum = get_file_checksum(filepath)
    load_time = datetime.utcnow().isoformat()

    print(f"  Loading encounters from {filepath} ...")
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .csv(filepath))

    # Add ingestion metadata
    df = (df
          .withColumn("_source", F.lit("encounters.csv"))
          .withColumn("_load_time", F.lit(load_time))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_file_checksum", F.lit(checksum)))

    # Write as Parquet, partitioned by report year-month for pruning
    df = df.withColumn("_report_year", F.substring("report_date", 1, 4))

    output_path = os.path.join(BRONZE_DIR, "encounters")
    df.repartition(SPARK_PARTITIONS).write.mode("overwrite") \
        .partitionBy("_report_year") \
        .parquet(output_path)

    count = df.count()
    print(f"  ✓ Bronze encounters: {count:,} rows → {output_path}")
    return count


def ingest_facilities(spark, batch_id):
    """Ingest facility registry CSV into bronze Parquet."""
    filepath = os.path.join(RAW_DIR, "facility_registry.csv")
    if not os.path.exists(filepath):
        print(f"  ⚠ facility_registry.csv not found, skipping")
        return

    checksum = get_file_checksum(filepath)
    load_time = datetime.utcnow().isoformat()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
    df = (df
          .withColumn("_source", F.lit("facility_registry.csv"))
          .withColumn("_load_time", F.lit(load_time))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_file_checksum", F.lit(checksum)))

    output_path = os.path.join(BRONZE_DIR, "facilities")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print(f"  ✓ Bronze facilities: {df.count()} rows → {output_path}")


def ingest_weather(spark, batch_id):
    """Ingest weather CSV into bronze Parquet."""
    filepath = os.path.join(RAW_DIR, "weather_daily.csv")
    if not os.path.exists(filepath):
        print(f"  ⚠ weather_daily.csv not found, skipping")
        return

    checksum = get_file_checksum(filepath)
    load_time = datetime.utcnow().isoformat()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
    df = (df
          .withColumn("_source", F.lit("weather_daily.csv"))
          .withColumn("_load_time", F.lit(load_time))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_file_checksum", F.lit(checksum)))

    output_path = os.path.join(BRONZE_DIR, "weather")
    df.repartition(4).write.mode("overwrite").parquet(output_path)

    print(f"  ✓ Bronze weather: {df.count()} rows → {output_path}")


def ingest_stock(spark, batch_id):
    """Ingest stock snapshots JSONL into bronze Parquet."""
    filepath = os.path.join(RAW_DIR, "stock_snapshots.jsonl")
    if not os.path.exists(filepath):
        print(f"  ⚠ stock_snapshots.jsonl not found, skipping")
        return

    checksum = get_file_checksum(filepath)
    load_time = datetime.utcnow().isoformat()

    df = spark.read.json(filepath)
    df = (df
          .withColumn("_source", F.lit("stock_snapshots.jsonl"))
          .withColumn("_load_time", F.lit(load_time))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_file_checksum", F.lit(checksum)))

    output_path = os.path.join(BRONZE_DIR, "stock")
    df.repartition(4).write.mode("overwrite").parquet(output_path)

    print(f"  ✓ Bronze stock: {df.count()} rows → {output_path}")


def run_bronze_pipeline():
    """Run the full bronze ingestion pipeline."""
    print("═══ Bronze Layer Ingestion ═══")
    batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    print(f"  Batch ID: {batch_id}")

    spark = get_spark()

    try:
        ingest_encounters(spark, batch_id)
        ingest_facilities(spark, batch_id)
        ingest_weather(spark, batch_id)
        ingest_stock(spark, batch_id)
        print("═══ Bronze Ingestion Complete ═══")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_pipeline()
