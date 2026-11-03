"""
EpiScope Resource Allocation Optimiser (AN-7, AN-8)
Formulates and solves a linear programme for optimal resource allocation
across districts under a fixed budget, with an equity floor constraint.
Compares with population-proportional baseline.
Produces efficiency–equity curve (conflict C3).
"""

import os
import sys
import json

import numpy as np
import pandas as pd
import pulp

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import (
    GOLD_DIR, SERVING_DIR, SPARK_APP_NAME, SPARK_PARTITIONS,
    DISTRICT_POPULATIONS, get_all_districts
)


RESOURCE_COSTS = {
    "iv_saline": 50,      # BDT per unit
    "ns1_kits": 200,      # BDT per kit
    "ors": 10,            # BDT per packet
    "antimalarials": 100, # BDT per dose
}

# Severity/vulnerability weights (higher = more priority)
VULNERABILITY_WEIGHTS = {
    "Dhaka": 1.0, "Chattogram": 1.0, "Cox's Bazar": 1.3,
    "Bandarban": 1.5, "Khagrachhari": 1.4, "Rangamati": 1.4,
    "Sunamganj": 1.3, "Kurigram": 1.3,
}


def get_spark():
    return (SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_optimiser")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
            .config("spark.driver.memory", "4g")
            .getOrCreate())


def solve_allocation(demand_df, stock_df, budget, alpha=0.0):
    """
    Solve the resource allocation LP.

    min  Σ_d Σ_r  w_d * u_{d,r}
    s.t. u_{d,r} >= ŷ_{d,r} - s_{d,r} - x_{d,r}    (unmet demand)
         Σ_d Σ_r c_r * x_{d,r} <= B                   (budget)
         x_{d,r} >= α * p_d * Σ_{d'} x_{d',r}         (equity floor)
         x_{d,r} >= 0, u_{d,r} >= 0

    Args:
        demand_df: DataFrame with columns [district, resource, forecast_demand]
        stock_df: DataFrame with columns [district, resource, current_stock]
        budget: Total budget in BDT
        alpha: Equity floor parameter (0 = pure efficiency, 1 = proportional)

    Returns:
        allocation dict, total unmet demand, model status
    """
    districts = sorted(demand_df["district"].unique())
    resources = sorted(demand_df["resource"].unique())

    # Population shares
    total_pop = sum(DISTRICT_POPULATIONS.get(d, 1500) for d in districts)
    pop_share = {d: DISTRICT_POPULATIONS.get(d, 1500) / total_pop for d in districts}

    # Vulnerability weights
    w = {d: VULNERABILITY_WEIGHTS.get(d, 1.0) for d in districts}

    # Create problem
    prob = pulp.LpProblem("EpiScope_Allocation", pulp.LpMinimize)

    # Decision variables
    x = {}  # allocation
    u = {}  # unmet demand
    for d in districts:
        for r in resources:
            x[d, r] = pulp.LpVariable(f"x_{d}_{r}", lowBound=0)
            u[d, r] = pulp.LpVariable(f"u_{d}_{r}", lowBound=0)

    # Objective: minimise weighted unmet demand
    prob += pulp.lpSum(w[d] * u[d, r] for d in districts for r in resources)

    # Constraints
    for d in districts:
        for r in resources:
            # Get demand and stock
            demand_row = demand_df[(demand_df["district"] == d) & (demand_df["resource"] == r)]
            stock_row = stock_df[(stock_df["district"] == d) & (stock_df["resource"] == r)]

            y_hat = demand_row["forecast_demand"].values[0] if len(demand_row) > 0 else 0
            s = stock_row["current_stock"].values[0] if len(stock_row) > 0 else 0

            # Unmet demand constraint
            prob += u[d, r] >= y_hat - s - x[d, r], f"unmet_{d}_{r}"

    # Budget constraint
    prob += pulp.lpSum(
        RESOURCE_COSTS.get(r, 50) * x[d, r] for d in districts for r in resources
    ) <= budget, "budget"

    # Equity floor constraint
    if alpha > 0:
        for d in districts:
            for r in resources:
                total_alloc_r = pulp.lpSum(x[d2, r] for d2 in districts)
                prob += x[d, r] >= alpha * pop_share[d] * total_alloc_r, f"equity_{d}_{r}"

    # Solve
    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    # Extract results
    allocation = {}
    for d in districts:
        allocation[d] = {}
        for r in resources:
            allocation[d][r] = {
                "allocated": round(pulp.value(x[d, r]) or 0, 1),
                "unmet": round(pulp.value(u[d, r]) or 0, 1),
            }

    total_unmet = sum(
        pulp.value(u[d, r]) or 0 for d in districts for r in resources
    )

    return allocation, total_unmet, pulp.LpStatus[prob.status]


def population_proportional_baseline(demand_df, stock_df, budget):
    """Population-proportional allocation baseline."""
    districts = sorted(demand_df["district"].unique())
    resources = sorted(demand_df["resource"].unique())

    total_pop = sum(DISTRICT_POPULATIONS.get(d, 1500) for d in districts)
    pop_share = {d: DISTRICT_POPULATIONS.get(d, 1500) / total_pop for d in districts}

    # Distribute budget proportionally
    allocation = {}
    total_unmet = 0

    for d in districts:
        allocation[d] = {}
        district_budget = budget * pop_share[d]

        for r in resources:
            demand_row = demand_df[(demand_df["district"] == d) & (demand_df["resource"] == r)]
            stock_row = stock_df[(stock_df["district"] == d) & (stock_df["resource"] == r)]

            y_hat = demand_row["forecast_demand"].values[0] if len(demand_row) > 0 else 0
            s = stock_row["current_stock"].values[0] if len(stock_row) > 0 else 0

            cost = RESOURCE_COSTS.get(r, 50)
            max_alloc = district_budget / (cost * len(resources)) if cost > 0 else 0
            alloc = min(max_alloc, max(0, y_hat - s))

            unmet = max(0, y_hat - s - alloc)
            allocation[d][r] = {
                "allocated": round(alloc, 1),
                "unmet": round(unmet, 1),
            }
            total_unmet += unmet

    return allocation, total_unmet


def run_optimiser():
    """Run the allocation optimiser."""
    print("═══ Resource Allocation Optimiser ═══")

    spark = get_spark()

    try:
        # Load forecasts
        forecast_path = os.path.join(GOLD_DIR, "forecasts")
        if not os.path.exists(forecast_path):
            print("  ⚠ Forecasts not found. Run forecasting first.")
            return

        forecasts = spark.read.parquet(forecast_path)

        # Aggregate demand: sum across diseases for 4-week horizon
        demand_agg = forecasts.filter(F.col("horizon_weeks") <= 4) \
            .groupBy("district") \
            .agg(
                F.sum("demand_beds").alias("beds"),
                F.sum("demand_iv_saline").alias("iv_saline"),
                F.sum("demand_ns1_kits").alias("ns1_kits"),
                F.sum("demand_ors").alias("ors"),
            )

        # Convert to pandas for optimisation (small data at this point)
        demand_pd = demand_agg.toPandas()

        # Reshape to long format
        demand_long = pd.melt(
            demand_pd,
            id_vars=["district"],
            value_vars=["iv_saline", "ns1_kits", "ors"],
            var_name="resource",
            value_name="forecast_demand"
        )
        demand_long["forecast_demand"] = demand_long["forecast_demand"].fillna(0)

        # Load stock summary
        stock_path = os.path.join(GOLD_DIR, "stock_summary")
        if os.path.exists(stock_path):
            stock_spark = spark.read.parquet(stock_path)
            # Get latest stock per district
            latest_stock = stock_spark.groupBy("district").agg(
                F.last("total_iv_saline").alias("iv_saline"),
                F.last("total_ns1_kits").alias("ns1_kits"),
                F.last("total_ors").alias("ors"),
            )
            stock_pd = latest_stock.toPandas()
            stock_long = pd.melt(
                stock_pd,
                id_vars=["district"],
                value_vars=["iv_saline", "ns1_kits", "ors"],
                var_name="resource",
                value_name="current_stock"
            )
            stock_long["current_stock"] = stock_long["current_stock"].fillna(0)
        else:
            # Default stock = 0
            stock_long = demand_long[["district", "resource"]].copy()
            stock_long["current_stock"] = 0

        # ── Solve at multiple budget levels (AN-8, conflict C3) ──
        base_budget = 50_000_000  # 50M BDT
        budget_levels = [0.25, 0.5, 0.75, 1.0, 1.5]
        equity_levels = [0.0, 0.1, 0.2, 0.3, 0.5]

        all_results = []
        efficiency_equity_curve = []

        for budget_mult in budget_levels:
            budget = int(base_budget * budget_mult)

            for alpha in equity_levels:
                print(f"  Solving: budget={budget/1e6:.1f}M BDT, α={alpha} ...")
                allocation, unmet, status = solve_allocation(
                    demand_long, stock_long, budget, alpha
                )

                # Population-proportional baseline
                pp_alloc, pp_unmet = population_proportional_baseline(
                    demand_long, stock_long, budget
                )

                result = {
                    "budget_bdt": budget,
                    "budget_multiplier": budget_mult,
                    "alpha": alpha,
                    "status": status,
                    "total_unmet_optimal": round(unmet, 1),
                    "total_unmet_proportional": round(pp_unmet, 1),
                    "improvement_pct": round((1 - unmet / max(pp_unmet, 1)) * 100, 1) if pp_unmet > 0 else 0,
                }
                efficiency_equity_curve.append(result)

                # Save detailed allocation for the default scenario
                if budget_mult == 1.0 and alpha == 0.1:
                    all_results = []
                    for d, resources in allocation.items():
                        for r, vals in resources.items():
                            all_results.append({
                                "district": d,
                                "resource": r,
                                "allocated": vals["allocated"],
                                "unmet": vals["unmet"],
                            })

        # Save allocation plan
        if all_results:
            alloc_df = spark.createDataFrame(pd.DataFrame(all_results))
            alloc_path = os.path.join(GOLD_DIR, "allocation_plan")
            alloc_df.write.mode("overwrite").parquet(alloc_path)
            print(f"  ✓ Allocation plan → {alloc_path}")

        # Save efficiency-equity curve
        curve_path = os.path.join(GOLD_DIR, "efficiency_equity_curve.json")
        with open(curve_path, "w") as f:
            json.dump(efficiency_equity_curve, f, indent=2)
        print(f"  ✓ Efficiency-equity curve → {curve_path}")

        # Print summary
        default_result = [r for r in efficiency_equity_curve
                         if r["budget_multiplier"] == 1.0 and r["alpha"] == 0.1]
        if default_result:
            r = default_result[0]
            print(f"\n  Default scenario (budget={r['budget_bdt']/1e6:.0f}M, α=0.1):")
            print(f"    Optimal unmet demand: {r['total_unmet_optimal']:.0f}")
            print(f"    Proportional baseline: {r['total_unmet_proportional']:.0f}")
            print(f"    Improvement: {r['improvement_pct']:.1f}%")

        print("═══ Optimiser Complete ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_optimiser()
