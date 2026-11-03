"""
EpiScope Serving Layer
Exports gold data to lightweight JSON files for the D3.js dashboard.
Applies small-cell suppression and ensures total data ≤ 10MB.
"""

import os
import sys
import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    GOLD_DIR, SERVING_DIR, SPARK_APP_NAME, SPARK_PARTITIONS,
    SMALL_CELL_THRESHOLD, DISTRICT_POPULATIONS, DIVISIONS
)


def get_spark():
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_serving")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .getOrCreate())


def suppress_value(val):
    """Suppress counts 1-4 for privacy."""
    if val is None:
        return None
    if isinstance(val, (int, float)) and 1 <= val < SMALL_CELL_THRESHOLD:
        return None
    return val


def export_serving_json():
    """Export all gold data as JSON for the dashboard."""
    print("═══ Serving Layer Export ═══")

    spark = get_spark()
    os.makedirs(SERVING_DIR, exist_ok=True)

    try:
        # 1. Epi-week time series
        mart_path = os.path.join(GOLD_DIR, "epiweek_mart")
        if os.path.exists(mart_path):
            mart = spark.read.parquet(mart_path)
            mart_pd = mart.toPandas()

            # Apply suppression and round floats
            for col in ["case_count", "lab_confirmed_count", "admissions", "deaths"]:
                if col in mart_pd.columns:
                    mart_pd[col] = mart_pd[col].apply(suppress_value)
            for float_col in ["mean_age", "incidence_per_100k"]:
                if float_col in mart_pd.columns:
                    mart_pd[float_col] = mart_pd[float_col].round(2)

            # Convert to JSON-friendly format
            ts_data = mart_pd.to_dict(orient="records")
            ts_path = os.path.join(SERVING_DIR, "timeseries.json")
            with open(ts_path, "w") as f:
                json.dump(ts_data, f, separators=(",", ":"))
            print(f"  ✓ Time series: {len(ts_data)} records → {ts_path}")
            print(f"    Size: {os.path.getsize(ts_path) / 1024 / 1024:.2f} MB")

        # 2. Detection alerts
        det_path = os.path.join(GOLD_DIR, "detection_results")
        if os.path.exists(det_path):
            det = spark.read.parquet(det_path)
            alerts = det.filter(F.col("alert_any") == True)
            alerts_pd = alerts.select(
                "district", "disease", "epi_year", "epi_week",
                "case_count", "z_c2", "z_c3", "cusum_s",
                "alert_ears_c2", "alert_ears_c3", "alert_cusum"
            ).toPandas()

            for float_col in ["z_c2", "z_c3", "cusum_s"]:
                if float_col in alerts_pd.columns:
                    alerts_pd[float_col] = alerts_pd[float_col].round(2)

            alerts_data = alerts_pd.to_dict(orient="records")
            alerts_path = os.path.join(SERVING_DIR, "alerts.json")
            with open(alerts_path, "w") as f:
                json.dump(alerts_data, f, separators=(",", ":"))
            print(f"  ✓ Alerts: {len(alerts_data)} records → {alerts_path}")

        # 3. Forecasts
        fc_path = os.path.join(GOLD_DIR, "forecasts")
        if os.path.exists(fc_path):
            fc = spark.read.parquet(fc_path)
            fc_pd = fc.toPandas()
            for col in fc_pd.select_dtypes(include=["float"]).columns:
                fc_pd[col] = fc_pd[col].round(2)
            fc_data = fc_pd.to_dict(orient="records")
            fc_file = os.path.join(SERVING_DIR, "forecasts.json")
            with open(fc_file, "w") as f:
                json.dump(fc_data, f, separators=(",", ":"))
            print(f"  ✓ Forecasts: {len(fc_data)} records → {fc_file}")

        # 4. Allocation plan
        alloc_path = os.path.join(GOLD_DIR, "allocation_plan")
        if os.path.exists(alloc_path):
            alloc = spark.read.parquet(alloc_path)
            alloc_pd = alloc.toPandas()
            alloc_data = alloc_pd.to_dict(orient="records")
            alloc_file = os.path.join(SERVING_DIR, "allocation.json")
            with open(alloc_file, "w") as f:
                json.dump(alloc_data, f, separators=(",", ":"))
            print(f"  ✓ Allocation: {len(alloc_data)} records → {alloc_file}")

        # 5. Preparedness calendar
        cal_path = os.path.join(GOLD_DIR, "preparedness_calendar")
        if os.path.exists(cal_path):
            cal = spark.read.parquet(cal_path)
            cal_pd = cal.toPandas()
            cal_data = cal_pd.to_dict(orient="records")
            cal_file = os.path.join(SERVING_DIR, "calendar.json")
            with open(cal_file, "w") as f:
                json.dump(cal_data, f, separators=(",", ":"))
            print(f"  ✓ Calendar: {len(cal_data)} records → {cal_file}")

        # 6. Detection evaluation
        for eval_file in ["detection_evaluation.json", "forecast_evaluation.json",
                          "threshold_tradeoff.json", "efficiency_equity_curve.json",
                          "calendar_summary.json"]:
            src = os.path.join(GOLD_DIR, eval_file)
            if os.path.exists(src):
                import shutil
                dst = os.path.join(SERVING_DIR, eval_file)
                shutil.copy2(src, dst)
                print(f"  ✓ Copied {eval_file}")

        # 7. DQ report
        dq_path = os.path.join(GOLD_DIR, "dq_report")
        if os.path.exists(dq_path):
            dq = spark.read.parquet(dq_path)
            dq_pd = dq.toPandas()
            dq_dict = dict(zip(dq_pd["metric"], dq_pd["value"]))
            dq_file = os.path.join(SERVING_DIR, "dq_report.json")
            with open(dq_file, "w") as f:
                json.dump(dq_dict, f, indent=2)
            print(f"  ✓ DQ report → {dq_file}")

        # 8. District metadata
        district_meta = []
        for div, districts in DIVISIONS.items():
            for d in districts:
                district_meta.append({
                    "district": d,
                    "division": div,
                    "population": DISTRICT_POPULATIONS.get(d, 1500) * 1000,
                })
        meta_file = os.path.join(SERVING_DIR, "districts.json")
        with open(meta_file, "w") as f:
            json.dump(district_meta, f)
        print(f"  ✓ District metadata → {meta_file}")

        # Check total size
        total_size = sum(
            os.path.getsize(os.path.join(SERVING_DIR, f))
            for f in os.listdir(SERVING_DIR)
            if os.path.isfile(os.path.join(SERVING_DIR, f))
        )
        print(f"\n  Total serving data: {total_size / 1024 / 1024:.2f} MB (limit: 10 MB)")

        if total_size > 10 * 1024 * 1024:
            print("  ⚠ WARNING: Serving data exceeds 10MB limit!")
        else:
            print("  ✓ Within 10MB limit")

        # Also mirror serving JSONs to dashboard/data for static web serving
        import shutil
        dash_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard", "data")
        os.makedirs(dash_data_dir, exist_ok=True)
        for fname in os.listdir(SERVING_DIR):
            if fname.endswith(".json"):
                shutil.copy2(os.path.join(SERVING_DIR, fname), os.path.join(dash_data_dir, fname))
        print(f"  ✓ Synced serving data to dashboard → {dash_data_dir}")

        print("═══ Serving Export Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    export_serving_json()
