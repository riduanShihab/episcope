"""
EpiScope — Master Pipeline Orchestrator
One-command execution: python run_all.py
Runs the full pipeline: generate → ingest → transform → aggregate → detect → forecast → optimise → calendar → serve
Supports scalability experiment via --sf flag.
"""

import os
import sys
import time
import argparse
from datetime import datetime

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Ensure project root is on path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config import DEFAULT_SF, DEFAULT_SEED


def run_stage(name, func, *args, **kwargs):
    """Run a pipeline stage with timing."""
    print(f"\n{'='*60}")
    print(f"  STAGE: {name}")
    print(f"{'='*60}")
    start = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"\n  ✓ {name} completed in {elapsed:.1f}s")
        return elapsed, True
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ✗ {name} FAILED after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return elapsed, False


def main():
    parser = argparse.ArgumentParser(
        description="EpiScope — End-to-end pipeline execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py                   # Run with SF=0.25 (development)
  python run_all.py --sf 1.0          # Run with SF=1.0 (full scale, 25M+ records)
  python run_all.py --sf 0.5 --seed 123
  python run_all.py --skip-generate   # Skip data generation (re-run pipeline only)
        """
    )
    parser.add_argument("--sf", type=float, default=DEFAULT_SF,
                        help=f"Scale factor (default: {DEFAULT_SF})")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help=f"Random seed (default: {DEFAULT_SEED})")
    parser.add_argument("--skip-generate", action="store_true",
                        help="Skip data generation (use existing raw data)")
    parser.add_argument("--skip-weather", action="store_true",
                        help="Skip weather data fetching")
    args = parser.parse_args()

    os.environ["EPISCOPE_SF"] = str(args.sf)
    os.environ["EPISCOPE_SEED"] = str(args.seed)

    print(f"""
============================================================
    EpiScope Pipeline
    Big Data Surveillance & Response-Planning Platform
============================================================

  Scale Factor: {args.sf}
  Seed: {args.seed}
  Started: {datetime.now().isoformat()}
  Hardware: Local Spark (standalone mode)
    """)

    timings = {}
    total_start = time.time()

    # ── Stage 1: Generate synthetic data ──
    if not args.skip_generate:
        from generator.generate import run_generator
        elapsed, ok = run_stage("1. Data Generation", run_generator, sf=args.sf, seed=args.seed)
        timings["generation"] = elapsed
        if not ok:
            print("FATAL: Generation failed. Aborting.")
            sys.exit(1)

    # ── Stage 2: Fetch weather data ──
    if not args.skip_weather and not args.skip_generate:
        from generator.fetch_weather import fetch_all_weather
        elapsed, ok = run_stage("2. Weather Data", fetch_all_weather, use_api=False)
        timings["weather"] = elapsed

    # ── Stage 3: Bronze ingestion ──
    from pipeline.bronze.ingest import run_bronze_pipeline
    elapsed, ok = run_stage("3. Bronze Ingestion", run_bronze_pipeline)
    timings["bronze"] = elapsed
    if not ok:
        print("FATAL: Bronze ingestion failed. Aborting.")
        sys.exit(1)

    # ── Stage 4: Silver transformation ──
    from pipeline.silver.transform import run_silver_pipeline
    elapsed, ok = run_stage("4. Silver Transformation", run_silver_pipeline)
    timings["silver"] = elapsed
    if not ok:
        print("FATAL: Silver transformation failed. Aborting.")
        sys.exit(1)

    # ── Stage 5: Gold aggregation ──
    from pipeline.gold.aggregate import run_gold_pipeline
    elapsed, ok = run_stage("5. Gold Aggregation", run_gold_pipeline)
    timings["gold"] = elapsed
    if not ok:
        print("FATAL: Gold aggregation failed. Aborting.")
        sys.exit(1)

    # ── Stage 6: Outbreak detection ──
    from analytics.detection.detect import detect_outbreaks
    elapsed, ok = run_stage("6. Outbreak Detection", detect_outbreaks)
    timings["detection"] = elapsed

    # ── Stage 7: Forecasting ──
    from analytics.forecast.forecast import run_forecasting
    elapsed, ok = run_stage("7. Forecasting", run_forecasting)
    timings["forecasting"] = elapsed

    # ── Stage 8: Resource allocation ──
    from analytics.optimise.optimise import run_optimiser
    elapsed, ok = run_stage("8. Allocation Optimiser", run_optimiser)
    timings["optimiser"] = elapsed

    # ── Stage 9: Preparedness calendar ──
    from analytics.calendar.calendar import run_calendar
    elapsed, ok = run_stage("9. Preparedness Calendar", run_calendar)
    timings["calendar"] = elapsed

    # ── Stage 10: Serving export ──
    from serving.export import export_serving_json
    elapsed, ok = run_stage("10. Serving Export", export_serving_json)
    timings["serving"] = elapsed

    # ── Summary ──
    total_elapsed = time.time() - total_start

    print(f"""
============================================================
    Pipeline Complete!
============================================================

  Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)
  Scale factor: {args.sf}

  Stage Timings:
    """)

    for stage, t in timings.items():
        bar = "#" * int(t / max(timings.values()) * 30) if timings.values() else ""
        print(f"    {stage:20s} {t:8.1f}s  {bar}")

    # Save timing report (for ENG-2 scalability experiment)
    import json
    timing_report = {
        "sf": args.sf,
        "seed": args.seed,
        "total_seconds": round(total_elapsed, 1),
        "timestamp": datetime.now().isoformat(),
        "stages": {k: round(v, 1) for k, v in timings.items()},
    }
    report_path = os.path.join(PROJECT_ROOT, "data", "serving", "timing_report.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(timing_report, f, indent=2)

    print(f"\n  Timing report: {report_path}")
    print(f"\n  To view the dashboard:")
    print(f"    cd dashboard && python -m http.server 8080")
    print(f"    Open: http://localhost:8080")


if __name__ == "__main__":
    main()
