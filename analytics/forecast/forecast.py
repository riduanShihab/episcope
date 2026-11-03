"""
EpiScope Forecasting Module (AN-4, AN-5, AN-6)
1-4 week-ahead weekly case forecasts per district for dengue and diarrhoea,
using lagged weather covariates. Trains per-district models in parallel with Spark.
Rolling-origin backtest against seasonal-naïve baseline.
Converts case forecasts to resource demand.
"""

import os
import sys
import json
import math
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, ArrayType
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import GOLD_DIR, SERVING_DIR, SPARK_APP_NAME, SPARK_PARTITIONS


# ── Resource conversion assumptions (AN-6) ──────────────────────────
RESOURCE_CONVERSION = {
    "dengue": {
        "beds_per_case": 0.35,
        "iv_saline_per_case": 2.0,
        "ns1_kits_per_case": 1.5,
        "ors_per_case": 0.5,
    },
    "diarrhoea": {
        "beds_per_case": 0.15,
        "iv_saline_per_case": 1.0,
        "ns1_kits_per_case": 0.0,
        "ors_per_case": 3.0,
    },
    "influenza": {
        "beds_per_case": 0.08,
        "iv_saline_per_case": 0.5,
        "ns1_kits_per_case": 0.0,
        "ors_per_case": 0.3,
    },
    "malaria": {
        "beds_per_case": 0.25,
        "iv_saline_per_case": 1.0,
        "ns1_kits_per_case": 0.0,
        "ors_per_case": 0.5,
    },
}


def get_spark():
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_forecast")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def seasonal_naive_forecast(series, horizon, season_length=52):
    """
    Seasonal-naïve baseline: forecast = value from same week last year.
    Returns (point_forecast, lower_80, upper_80).
    """
    forecasts = []
    for h in range(1, horizon + 1):
        idx = len(series) - season_length + h - 1
        if 0 <= idx < len(series):
            point = series[idx]
        else:
            point = np.mean(series[-season_length:]) if len(series) >= season_length else np.mean(series)

        # Simple prediction interval based on historical errors
        std = np.std(series[-season_length:]) if len(series) >= season_length else np.std(series)
        lower = max(0, point - 1.28 * std)  # 80% interval
        upper = point + 1.28 * std
        forecasts.append((float(point), float(lower), float(upper)))

    return forecasts


def linear_regression_forecast(y, X_weather, horizon):
    """
    Simple linear regression with weather covariates + trend + seasonality.
    Uses last available weather for forecast period.
    """
    n = len(y)
    if n < 20:
        return seasonal_naive_forecast(y, horizon)

    # Build feature matrix: trend + sin/cos seasonal + weather
    t = np.arange(n, dtype=float)
    features = [
        np.ones(n),
        t,
        np.sin(2 * np.pi * t / 52),
        np.cos(2 * np.pi * t / 52),
    ]

    if X_weather is not None and len(X_weather) == n:
        for col in range(X_weather.shape[1] if X_weather.ndim > 1 else 1):
            if X_weather.ndim > 1:
                features.append(np.nan_to_num(X_weather[:, col], nan=0.0))
            else:
                features.append(np.nan_to_num(X_weather, nan=0.0))

    X = np.column_stack(features)

    # Fit OLS
    try:
        beta, residuals, _, _ = np.linalg.lstsq(X, y, rcond=None)
        y_hat = X @ beta
        residual_std = np.std(y - y_hat)
    except Exception:
        return seasonal_naive_forecast(y, horizon)

    # Forecast
    forecasts = []
    for h in range(1, horizon + 1):
        t_new = n + h - 1
        x_new = [1, t_new, np.sin(2 * np.pi * t_new / 52), np.cos(2 * np.pi * t_new / 52)]

        # Use last available weather values
        if X_weather is not None and len(X_weather) > 0:
            if X_weather.ndim > 1:
                for col in range(X_weather.shape[1]):
                    x_new.append(float(np.nan_to_num(X_weather[-1, col], nan=0.0)))
            else:
                x_new.append(float(np.nan_to_num(X_weather[-1], nan=0.0)))

        x_new = np.array(x_new)
        if len(x_new) == len(beta):
            point = float(max(0, x_new @ beta))
        else:
            point = float(max(0, y_hat[-1] if len(y_hat) > 0 else np.mean(y)))

        lower = max(0, point - 1.28 * residual_std)
        upper = point + 1.28 * residual_std
        forecasts.append((point, lower, upper))

    return forecasts


def forecast_district_disease(pdf):
    """
    Pandas UDF for per-district per-disease forecasting.
    Called via groupedApplyInPandas.
    """
    pdf = pdf.sort_values(["epi_year", "epi_week"]).reset_index(drop=True)

    district = pdf["district"].iloc[0]
    disease = pdf["disease"].iloc[0]

    y = pdf["case_count"].fillna(0).values.astype(float)

    # Weather covariates
    weather_cols = [c for c in pdf.columns if c.startswith(("avg_temp", "total_precip", "precip_lag", "temp_lag"))]
    X_weather = pdf[weather_cols].fillna(0).values if weather_cols else None

    horizon = 4  # 4-week forecast

    # Model forecast
    model_fc = linear_regression_forecast(y, X_weather, horizon)

    # Seasonal-naïve baseline
    naive_fc = seasonal_naive_forecast(y, horizon)

    # Rolling backtest (last 52 weeks)
    backtest_start = max(52, len(y) - 52)
    model_errors = []
    naive_errors = []
    model_coverage = []

    for t in range(backtest_start, len(y)):
        if t < 20:
            continue
        y_train = y[:t]
        X_train = X_weather[:t] if X_weather is not None else None
        actual = y[t] if t < len(y) else 0

        # 1-step forecast
        m_fc = linear_regression_forecast(y_train, X_train, 1)
        n_fc = seasonal_naive_forecast(y_train, 1)

        model_errors.append(abs(m_fc[0][0] - actual))
        naive_errors.append(abs(n_fc[0][0] - actual))

        # Coverage check (80% PI)
        in_interval = m_fc[0][1] <= actual <= m_fc[0][2]
        model_coverage.append(1 if in_interval else 0)

    model_mae = float(np.mean(model_errors)) if model_errors else 0.0
    naive_mae = float(np.mean(naive_errors)) if naive_errors else 0.0
    coverage_80 = float(np.mean(model_coverage)) if model_coverage else 0.0

    # Build results
    last_year = int(pdf["epi_year"].iloc[-1])
    last_week = int(pdf["epi_week"].iloc[-1])

    results = []
    for h in range(horizon):
        fc_week = last_week + h + 1
        fc_year = last_year
        if fc_week > 52:
            fc_week -= 52
            fc_year += 1

        # Resource demand conversion (AN-6)
        point_cases = model_fc[h][0]
        conv = RESOURCE_CONVERSION.get(disease, RESOURCE_CONVERSION["influenza"])

        results.append({
            "district": district,
            "disease": disease,
            "forecast_year": fc_year,
            "forecast_week": fc_week,
            "horizon_weeks": h + 1,
            "point_forecast": round(model_fc[h][0], 1),
            "lower_80": round(model_fc[h][1], 1),
            "upper_80": round(model_fc[h][2], 1),
            "naive_forecast": round(naive_fc[h][0], 1),
            "model_mae": round(model_mae, 2),
            "naive_mae": round(naive_mae, 2),
            "coverage_80": round(coverage_80, 4),
            # Resource demand (AN-6)
            "demand_beds": round(point_cases * conv["beds_per_case"], 1),
            "demand_iv_saline": round(point_cases * conv["iv_saline_per_case"], 1),
            "demand_ns1_kits": round(point_cases * conv["ns1_kits_per_case"], 1),
            "demand_ors": round(point_cases * conv["ors_per_case"], 1),
        })

    return pd.DataFrame(results)


def run_forecasting():
    """Run forecasting for all districts and diseases."""
    print("═══ Forecasting ═══")

    spark = get_spark()

    try:
        # Load feature table
        feature_path = os.path.join(GOLD_DIR, "feature_table")
        if not os.path.exists(feature_path):
            print("  ⚠ Gold feature table not found. Run gold pipeline first.")
            return

        df = spark.read.parquet(feature_path)
        df = df.withColumn("case_count", F.coalesce(F.col("case_count"), F.lit(0)))

        # Filter to target diseases
        target_diseases = ["dengue", "diarrhoea", "influenza", "malaria"]
        df = df.filter(F.col("disease").isin(target_diseases))
        print(f"  Loaded {df.count():,} feature records for {target_diseases}")

        # Define output schema
        result_schema = StructType([
            StructField("district", StringType()),
            StructField("disease", StringType()),
            StructField("forecast_year", IntegerType()),
            StructField("forecast_week", IntegerType()),
            StructField("horizon_weeks", IntegerType()),
            StructField("point_forecast", FloatType()),
            StructField("lower_80", FloatType()),
            StructField("upper_80", FloatType()),
            StructField("naive_forecast", FloatType()),
            StructField("model_mae", FloatType()),
            StructField("naive_mae", FloatType()),
            StructField("coverage_80", FloatType()),
            StructField("demand_beds", FloatType()),
            StructField("demand_iv_saline", FloatType()),
            StructField("demand_ns1_kits", FloatType()),
            StructField("demand_ors", FloatType()),
        ])

        # Run per-district per-disease using applyInPandas (AN-4) with fallback
        print("  Running per-district forecasting ...")
        try:
            forecasts = df.groupBy("district", "disease").applyInPandas(
                forecast_district_disease, schema=result_schema
            )
            # Verify execution
            _ = forecasts.first()
        except Exception as e:
            print(f"  Info: applyInPandas fallback engaged ({e.__class__.__name__}). Processing in pandas ...")
            pdf = df.toPandas()
            results = []
            for (district, disease), group in pdf.groupby(["district", "disease"]):
                res = forecast_district_disease(group)
                results.append(res)
            all_pdf = pd.concat(results, ignore_index=True)
            forecasts = spark.createDataFrame(all_pdf, schema=result_schema)

        # Save forecasts
        forecast_path = os.path.join(GOLD_DIR, "forecasts")
        forecasts.write.mode("overwrite").parquet(forecast_path)
        fc_count = forecasts.count()
        print(f"  ✓ Forecasts: {fc_count:,} rows → {forecast_path}")

        # ── Backtest summary (AN-5) ──
        print("  Computing backtest summary ...")
        backtest = forecasts.filter(F.col("horizon_weeks") == 1) \
                            .groupBy("disease").agg(
                                F.avg("model_mae").alias("avg_model_mae"),
                                F.avg("naive_mae").alias("avg_naive_mae"),
                                F.avg("coverage_80").alias("avg_coverage_80"),
                            )

        backtest_results = {}
        for row in backtest.collect():
            backtest_results[row["disease"]] = {
                "model_mae": round(row["avg_model_mae"], 2),
                "naive_mae": round(row["avg_naive_mae"], 2),
                "coverage_80": round(row["avg_coverage_80"], 4),
                "skill_score": round(1 - row["avg_model_mae"] / max(row["avg_naive_mae"], 0.01), 4),
            }

        eval_path = os.path.join(GOLD_DIR, "forecast_evaluation.json")
        with open(eval_path, "w") as f:
            json.dump(backtest_results, f, indent=2)
        print(f"  ✓ Backtest evaluation → {eval_path}")

        for disease, metrics in backtest_results.items():
            print(f"    {disease}: MAE={metrics['model_mae']:.1f} vs naïve={metrics['naive_mae']:.1f}, "
                  f"coverage={metrics['coverage_80']:.1%}, skill={metrics['skill_score']:.3f}")

        print("═══ Forecasting Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_forecasting()
