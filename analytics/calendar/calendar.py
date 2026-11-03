"""
EpiScope Seasonal Preparedness Calendar (AN-9)
Produces a seasonal preparedness calendar for each division and disease,
showing when to pre-position supplies and start prevention campaigns.
Derived from seasonal decomposition of historical data.
"""

import os
import sys

# Prevent shadowing Python standard library 'calendar' module when run directly
_cur_dir = os.path.dirname(os.path.abspath(__file__))
if sys.path and os.path.abspath(sys.path[0]) == _cur_dir:
    sys.path.pop(0)
import calendar as _stdlib_calendar

# Ensure project root is on sys.path
_project_root = os.path.dirname(os.path.dirname(_cur_dir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import json
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import GOLD_DIR, SPARK_APP_NAME, SPARK_PARTITIONS, DIVISIONS


def get_spark():
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_calendar")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .getOrCreate())


def seasonal_decompose_simple(series, period=52):
    """Simple additive seasonal decomposition."""
    n = len(series)
    if n < period:
        return np.zeros(period)

    # Trend: centered moving average
    trend = pd.Series(series).rolling(window=period, center=True, min_periods=1).mean().values

    # De-trended
    detrended = series - trend

    # Seasonal: average of de-trended values for each position
    seasonal = np.zeros(period)
    counts = np.zeros(period)
    for i in range(n):
        pos = i % period
        if not np.isnan(detrended[i]):
            seasonal[pos] += detrended[i]
            counts[pos] += 1

    for i in range(period):
        if counts[i] > 0:
            seasonal[i] /= counts[i]

    return seasonal


def classify_risk_level(seasonal_value, mean_seasonal, std_seasonal):
    """Classify epi-week risk level."""
    if std_seasonal == 0:
        return "low"
    z = (seasonal_value - mean_seasonal) / std_seasonal
    if z > 1.5:
        return "very_high"
    elif z > 0.75:
        return "high"
    elif z > 0:
        return "moderate"
    else:
        return "low"


def run_calendar():
    """Build the seasonal preparedness calendar."""
    print("═══ Seasonal Preparedness Calendar ═══")

    spark = get_spark()

    try:
        # Load epi-week mart
        mart_path = os.path.join(GOLD_DIR, "epiweek_mart")
        if not os.path.exists(mart_path):
            print("  ⚠ Epi-week mart not found. Run gold pipeline first.")
            return

        df = spark.read.parquet(mart_path)
        df = df.withColumn("case_count", F.coalesce(F.col("case_count"), F.lit(0)))

        # Aggregate by division
        df = df.groupBy("division", "disease", "epi_year", "epi_week").agg(
            F.sum("case_count").alias("case_count")
        )

        # Convert to pandas for decomposition
        pdf = df.toPandas()

        calendar = []
        diseases = pdf["disease"].unique()
        divisions = pdf["division"].unique()

        for division in divisions:
            for disease in diseases:
                subset = pdf[(pdf["division"] == division) & (pdf["disease"] == disease)]
                subset = subset.sort_values(["epi_year", "epi_week"])

                series = subset["case_count"].values.astype(float)
                if len(series) < 52:
                    continue

                # Seasonal decomposition
                seasonal = seasonal_decompose_simple(series, period=52)

                mean_s = np.mean(seasonal)
                std_s = np.std(seasonal)

                # Find peak and trough weeks
                peak_week = int(np.argmax(seasonal)) + 1
                trough_week = int(np.argmin(seasonal)) + 1

                # Pre-position: 4-6 weeks before peak
                preposition_start = (peak_week - 6) % 52 + 1
                preposition_end = (peak_week - 4) % 52 + 1

                # Prevention campaign: 8-4 weeks before peak
                prevention_start = (peak_week - 8) % 52 + 1
                prevention_end = (peak_week - 4) % 52 + 1

                # Build weekly risk profile
                for week in range(1, 53):
                    risk = classify_risk_level(seasonal[week - 1], mean_s, std_s)
                    actions = []

                    if prevention_start <= week <= prevention_end or \
                       (prevention_start > prevention_end and (week >= prevention_start or week <= prevention_end)):
                        actions.append("prevention_campaign")

                    if preposition_start <= week <= preposition_end or \
                       (preposition_start > preposition_end and (week >= preposition_start or week <= preposition_end)):
                        actions.append("pre_position_supplies")

                    if week == peak_week:
                        actions.append("peak_surveillance")

                    calendar.append({
                        "division": division,
                        "disease": disease,
                        "epi_week": week,
                        "seasonal_index": round(float(seasonal[week - 1]), 2),
                        "risk_level": risk,
                        "actions": "|".join(actions) if actions else "routine",
                        "peak_week": peak_week,
                        "trough_week": trough_week,
                    })

        # Save calendar
        calendar_df = spark.createDataFrame(pd.DataFrame(calendar))
        calendar_path = os.path.join(GOLD_DIR, "preparedness_calendar")
        calendar_df.write.mode("overwrite").parquet(calendar_path)
        print(f"  ✓ Preparedness calendar: {len(calendar)} rows → {calendar_path}")

        # Summary JSON for dashboard
        summary = {}
        for division in divisions:
            summary[division] = {}
            for disease in diseases:
                entries = [c for c in calendar
                          if c["division"] == division and c["disease"] == disease]
                if entries:
                    summary[division][disease] = {
                        "peak_week": entries[0]["peak_week"],
                        "trough_week": entries[0]["trough_week"],
                        "high_risk_weeks": [e["epi_week"] for e in entries if e["risk_level"] in ("high", "very_high")],
                    }

        summary_path = os.path.join(GOLD_DIR, "calendar_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"  ✓ Calendar summary → {summary_path}")

        print("═══ Calendar Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_calendar()
