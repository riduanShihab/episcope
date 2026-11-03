"""
EpiScope Outbreak Detection (AN-1, AN-2, AN-3)
Implements EARS C1/C2/C3 and CUSUM methods at scale using Spark window functions.
Evaluates against ground_truth_outbreaks.
"""

import os
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import GOLD_DIR, SERVING_DIR, SPARK_APP_NAME, SPARK_PARTITIONS, RAW_DIR


def get_spark():
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_detection")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def ears_c1c2c3(df):
    """
    EARS C1/C2/C3 detection (CDC Early Aberration Reporting System).

    C1: z-score over baseline of weeks t-7 to t-1
    C2: z-score over baseline of weeks t-9 to t-3 (2-week guard band)
    C3: C2 + cumulative sum of recent C2 scores
    """
    w = Window.partitionBy("district", "disease").orderBy("epi_year", "epi_week")

    # Rolling baseline statistics
    # C1: 7-week window ending at t-1
    df = df.withColumn("baseline_mean_c1",
                       F.avg("case_count").over(w.rowsBetween(-7, -1)))
    df = df.withColumn("baseline_std_c1",
                       F.stddev("case_count").over(w.rowsBetween(-7, -1)))

    # C2: 7-week window ending at t-3 (guard band of 2)
    df = df.withColumn("baseline_mean_c2",
                       F.avg("case_count").over(w.rowsBetween(-9, -3)))
    df = df.withColumn("baseline_std_c2",
                       F.stddev("case_count").over(w.rowsBetween(-9, -3)))

    # Z-scores
    df = df.withColumn("z_c1",
                       F.when(F.col("baseline_std_c1") > 0,
                              (F.col("case_count") - F.col("baseline_mean_c1")) / F.col("baseline_std_c1"))
                       .otherwise(0))

    df = df.withColumn("z_c2",
                       F.when(F.col("baseline_std_c2") > 0,
                              (F.col("case_count") - F.col("baseline_mean_c2")) / F.col("baseline_std_c2"))
                       .otherwise(0))

    # C3: cumulative sum of max(0, z_c2 - 1) over last 3 weeks
    df = df.withColumn("z_c2_excess", F.greatest(F.col("z_c2") - 1, F.lit(0)))
    df = df.withColumn("z_c3",
                       F.col("z_c2") + F.sum("z_c2_excess").over(w.rowsBetween(-3, -1)))

    return df


def cusum_detection(df, k=0.5, h=5.0):
    """
    CUSUM (Cumulative Sum) detection.
    Detects sustained shifts in the mean.
    k: allowance parameter (typically 0.5-1.0 sigma)
    h: threshold parameter (typically 4-5 sigma)
    """
    w = Window.partitionBy("district", "disease").orderBy("epi_year", "epi_week")

    # Historical baseline (52-week trailing mean and std)
    df = df.withColumn("baseline_mean_52w",
                       F.avg("case_count").over(w.rowsBetween(-52, -1)))
    df = df.withColumn("baseline_std_52w",
                       F.stddev("case_count").over(w.rowsBetween(-52, -1)))

    # Standardized deviation
    df = df.withColumn("cusum_z",
                       F.when(F.col("baseline_std_52w") > 0,
                              (F.col("case_count") - F.col("baseline_mean_52w")) / F.col("baseline_std_52w") - k)
                       .otherwise(0))

    # Cumulative sum (simplified – reset on negative)
    # Using rolling sum as approximation for Spark scalability
    df = df.withColumn("cusum_s",
                       F.sum(F.greatest(F.col("cusum_z"), F.lit(0))).over(w.rowsBetween(-4, 0)))

    df = df.withColumn("cusum_alert", F.col("cusum_s") > h)

    return df


def detect_outbreaks(threshold_c2=2.0, threshold_c3=2.5):
    """Run outbreak detection and evaluate against ground truth."""
    print("═══ Outbreak Detection ═══")

    spark = get_spark()

    try:
        # Load gold epi-week mart
        feature_path = os.path.join(GOLD_DIR, "epiweek_mart")
        if not os.path.exists(feature_path):
            print("  ⚠ Gold epi-week mart not found. Run gold pipeline first.")
            return

        df = spark.read.parquet(feature_path)

        # Fill nulls in case_count (suppressed small cells) with 0 for detection
        df = df.withColumn("case_count",
                           F.coalesce(F.col("case_count"), F.lit(0)))

        print(f"  Loaded {df.count():,} epi-week records")

        # Apply EARS C1/C2/C3
        print("  Applying EARS C1/C2/C3 detection ...")
        df = ears_c1c2c3(df)

        # Apply CUSUM
        print("  Applying CUSUM detection ...")
        df = cusum_detection(df)

        # Generate alerts (AN-1)
        df = df.withColumn("alert_ears_c2", F.col("z_c2") > threshold_c2) \
               .withColumn("alert_ears_c3", F.col("z_c3") > threshold_c3) \
               .withColumn("alert_cusum", F.col("cusum_alert"))

        # Combined alert (either method)
        df = df.withColumn("alert_any",
                           F.col("alert_ears_c2") | F.col("alert_ears_c3") | F.col("alert_cusum"))

        # Save detection results
        detection_path = os.path.join(GOLD_DIR, "detection_results")
        df.write.mode("overwrite").partitionBy("epi_year").parquet(detection_path)
        print(f"  ✓ Detection results → {detection_path}")

        # Count alerts
        alerts = df.filter(F.col("alert_any"))
        print(f"  Total alerts: {alerts.count():,}")

        # ── Evaluate against ground truth (AN-2) ──
        gt_path = os.path.join(RAW_DIR, "ground_truth_outbreaks.csv")
        if os.path.exists(gt_path):
            print("  Evaluating against ground truth ...")
            gt = spark.read.option("header", "true").option("inferSchema", "true").csv(gt_path)

            # Expand ground truth to week-level
            gt_expanded_rows = []
            for row in gt.collect():
                for w in range(row["start_week"], row["start_week"] + row["duration_weeks"]):
                    district = row["district"] if row["district"] != "ALL" else None
                    gt_expanded_rows.append((
                        row["outbreak_id"], row["disease"], district,
                        row["year"], w, row["type"]
                    ))

            gt_expanded = spark.createDataFrame(
                gt_expanded_rows,
                ["outbreak_id", "gt_disease", "gt_district", "gt_year", "gt_week", "gt_type"]
            )

            # Join detection with ground truth
            gt_district = gt_expanded.filter(F.col("gt_district").isNotNull())
            gt_nationwide = gt_expanded.filter(F.col("gt_district").isNull())

            # District-specific outbreaks
            eval_district = df.join(
                gt_district,
                (df.district == gt_district.gt_district) &
                (df.disease == gt_district.gt_disease) &
                (df.epi_year == gt_district.gt_year) &
                (df.epi_week == gt_district.gt_week),
                how="left"
            )

            # Metrics
            true_positives = eval_district.filter(
                F.col("alert_any") & F.col("outbreak_id").isNotNull()
            ).count()

            false_positives = df.filter(F.col("alert_any")).count() - true_positives
            false_negatives = gt_expanded.count() - true_positives

            sensitivity = true_positives / max(true_positives + false_negatives, 1)
            ppv = true_positives / max(true_positives + false_positives, 1)

            # Days to detection (first alert within each outbreak)
            first_alert = eval_district.filter(
                F.col("alert_any") & F.col("outbreak_id").isNotNull()
            ).groupBy("outbreak_id").agg(
                F.min("epi_week").alias("first_alert_week")
            )

            gt_starts = gt_expanded.groupBy("outbreak_id").agg(
                F.min("gt_week").alias("start_week")
            )

            detection_delay = first_alert.join(gt_starts, on="outbreak_id")
            detection_delay = detection_delay.withColumn(
                "delay_weeks", F.col("first_alert_week") - F.col("start_week")
            )

            if detection_delay.count() > 0:
                median_delay = detection_delay.approxQuantile("delay_weeks", [0.5], 0.01)[0]
            else:
                median_delay = -1

            eval_results = {
                "threshold_c2": threshold_c2,
                "threshold_c3": threshold_c3,
                "true_positives": true_positives,
                "false_positives": false_positives,
                "false_negatives": false_negatives,
                "sensitivity": round(sensitivity, 4),
                "positive_predictive_value": round(ppv, 4),
                "median_delay_weeks": median_delay,
                "total_outbreaks_in_gt": gt.count(),
            }

            eval_path = os.path.join(GOLD_DIR, "detection_evaluation.json")
            with open(eval_path, "w") as f:
                json.dump(eval_results, f, indent=2)
            print(f"  ✓ Evaluation: sensitivity={sensitivity:.3f}, PPV={ppv:.3f}, "
                  f"median delay={median_delay:.1f} weeks")
            print(f"  ✓ Evaluation saved → {eval_path}")

            # ── Threshold trade-off curve (AN-2, conflict C1) ──
            print("  Computing threshold trade-off curve ...")
            tradeoff = []
            for t in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
                n_alerts = df.filter(F.col("z_c2") > t).count()
                n_tp = eval_district.filter(
                    (F.col("z_c2") > t) & F.col("outbreak_id").isNotNull()
                ).count()
                n_fp = n_alerts - n_tp
                n_fn = gt_expanded.count() - n_tp
                sens = n_tp / max(n_tp + n_fn, 1)
                spec = 1 - n_fp / max(df.count() - gt_expanded.count(), 1)
                tradeoff.append({
                    "threshold": t,
                    "sensitivity": round(sens, 4),
                    "specificity": round(spec, 4),
                    "false_alerts": n_fp,
                    "total_alerts": n_alerts,
                })

            tradeoff_path = os.path.join(GOLD_DIR, "threshold_tradeoff.json")
            with open(tradeoff_path, "w") as f:
                json.dump(tradeoff, f, indent=2)
            print(f"  ✓ Threshold trade-off curve → {tradeoff_path}")

        print("═══ Detection Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    detect_outbreaks()
