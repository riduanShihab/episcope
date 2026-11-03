# EpiScope — Build Progress Tracker

> **Project**: EpiScope — Big Data Surveillance, Forecasting & Response-Planning Platform
> **Course**: CSE 4345 – Big Data Analytics (CEP Assignment)
> **Last Updated**: 2026-10-27T23:36:00+06:00

---

## ✅ Completed Components

### 1. Project Setup
- [x] Directory structure created (follows Appendix A layout)
- [x] `requirements.txt` — pinned dependencies (pyspark, pandas, numpy, scipy, pulp, requests, pyarrow)
- [x] Dependencies installed: pyspark 4.2.0, pulp 3.3.2, py4j 0.10.9.9
- [x] Python 3.11, Java 8, Node 22 verified on system

### 2. Configuration (`config.py`)
- [x] All 64 Bangladesh districts with divisions, populations, centroids
- [x] Disease parameters: ICD-10 codes, seasonal peaks, base incidence, severity rates
- [x] Epidemiological constants: reporting delay, duplication rate, quality defect rates
- [x] Path constants: RAW, BRONZE, SILVER, GOLD, SERVING directories
- [x] Privacy settings: small-cell threshold (5), hash algorithm (SHA-256)

### 3. Synthetic Data Generator (`generator/generate.py`)
- [x] **S4**: Encounter line list generator
  - Deterministic via numpy seed
  - SF=1 → ≥25M records across 64 districts, 2021–2025
  - Seasonal von-Mises profiles per disease
  - Climate-driven incidence (rainfall/temp lag)
  - Negative-binomial overdispersion
  - 30+ injected outbreaks (localized + slow-growing + nationwide respiratory)
  - Reporting artefacts (facility silence + backfill)
  - 2% duplicates, 0.5% orphan FKs, 1% invalid ages
- [x] **S5**: Facility registry (≥600 facilities with type, beds, ICU, location)
- [x] **S6**: Daily stock snapshots (IV saline, NS1 kits, ORS, antimalarials, bed occupancy)
- [x] Ground truth outbreaks file (separate, never read by detection)

### 4. Weather Data (`generator/fetch_weather.py`)
- [x] **S3**: Open-Meteo API integration with rate limiting + caching
- [x] Synthetic weather fallback (temperature, precipitation, humidity with seasonal patterns)
- [x] 64 district centroids with coordinates
- [x] Outputs consolidated `weather_daily.csv`

### 5. Bronze Pipeline (`pipeline/bronze/ingest.py`)
- [x] **ING-1**: Ingests S1–S6 into bronze layer without modification
- [x] Adds ingestion metadata: source, load_time, batch_id, file_checksum (SHA-256)
- [x] **ING-2**: Uses Apache Spark (PySpark) for all transforms
- [x] **ING-3**: Stores as Parquet, partitioned by report year
- [x] Handles CSV (encounters, facilities, weather) and JSONL (stock)

### 6. Silver Pipeline (`pipeline/silver/transform.py`)
- [x] **DQ-1**: 13 automated data quality rules:
  - R01–R05: Schema completeness (nulls in encounter_id, patient_id, facility_id, onset_date, icd10_code)
  - R06: Valid age range (0 < age < 120)
  - R07: Valid sex (M/F)
  - R08: onset_date ≤ report_date
  - R09: Valid ICD-10 code
  - R10: Referential integrity (facility_id exists)
  - R11: Date range validity
  - R12: Duplicate encounter_id detection
  - R13: District validation
- [x] **DQ-2**: Per-load data quality report (JSON with pass rates)
- [x] Quarantine table with reason codes
- [x] Deduplication (keep first by load time)
- [x] **PRV-1**: Patient de-identification (salted SHA-256 hash, salt from env var)
- [x] Age banding (5-year bands)
- [x] Epi-year/epi-week derivation

### 7. Gold Pipeline (`pipeline/gold/aggregate.py`)
- [x] Epi-week mart: weekly cases by district × disease with incidence/100k
- [x] Weather feature engineering: weekly aggregates + lagged features (4/6/8-week lags)
- [x] Stock summary: weekly district-level stock + bed occupancy
- [x] Joined feature table (epi-week + weather + stock)
- [x] **PRV-2**: Small-cell suppression (counts 1–4 → null)
- [x] DQ report stored as gold table

### 8. Outbreak Detection (`analytics/detection/detect.py`)
- [x] **AN-1**: EARS C1/C2/C3 detection using Spark window functions
- [x] CUSUM detection method
- [x] **AN-2**: Evaluation against ground_truth_outbreaks (sensitivity, PPV, median delay)
- [x] **AN-3**: Comparison of two methods (EARS vs CUSUM)
- [x] Threshold trade-off curve (conflict C1: sensitivity vs specificity)

### 9. Forecasting (`analytics/forecast/forecast.py`)
- [x] **AN-4**: 1–4 week-ahead forecasts per district × disease
- [x] Linear regression with weather covariates + trend + seasonality
- [x] Per-district models via Spark `applyInPandas`
- [x] **AN-5**: Rolling-origin backtest vs seasonal-naïve baseline (MAE, 80% PI coverage)
- [x] **AN-6**: Resource demand conversion (beds, IV saline, NS1 kits, ORS)

### 10. Allocation Optimiser (`analytics/optimise/optimise.py`)
- [x] **AN-7**: LP formulation with PuLP/CBC solver
  - Minimises weighted unmet demand
  - Budget constraint
  - Equity floor constraint (α parameter)
  - Vulnerability weights per district
- [x] Population-proportional baseline comparison
- [x] **AN-8**: Efficiency–equity curve across 5 budget levels × 5 α values

### 11. Preparedness Calendar (`analytics/calendar/calendar.py`)
- [x] **AN-9**: Seasonal decomposition per division × disease
- [x] Risk classification (low/moderate/high/very_high)
- [x] Pre-positioning and prevention campaign timing
- [x] Peak/trough week identification

### 12. Serving Layer (`serving/export.py`)
- [x] Gold → JSON export for dashboard
- [x] **VIS-4**: Aggregated outputs only (enforces ≤10MB limit)
- [x] **PRV-2**: Small-cell suppression in all outputs
- [x] Exports: timeseries, alerts, forecasts, allocation, calendar, DQ report, districts metadata

---

## 🔨 In Progress / Remaining

### 13. D3.js Dashboard (`dashboard/`)
- [x] `index.html` — main page with responsive layout and dark theme
- [x] D3.js v7 modules for 5 coordinated views:
  - [x] **VIS-2(1)**: District choropleth map with alert markers and risk level tooltips
  - [x] **VIS-2(2)**: Time series chart with zoom/filter, forecast bands, and alert indicators
  - [x] **VIS-2(3)**: Seasonal heatmap (epi-week × year) showing disease intensity
  - [x] **VIS-2(4)**: Allocation view (recommended vs unmet demand across resources)
  - [x] **VIS-2(5)**: Ranked early-warning alert table with severity and method tags
- [x] **VIS-3**: Coordinated bidirectional filter interactions (year, disease, district)
- [x] **VIS-4**: Serving layer strictly aggregated and ≤ 10MB limit (8.92 MB verified)
- [x] **VIS-6**: Accessible color-blind-safe palettes, uncertainty prediction intervals, live freshness timestamps
- [x] Verified in browser with full 200 OK responses on all endpoints

### 14. Orchestration & Run Scripts
- [x] `run_all.py` — one-command master pipeline execution (ENG-1) tested end-to-end
- [x] Generates scalability and execution timing reports (`timing_report.json`)
- [x] `Makefile` — pipeline targets (all, generate, bronze, silver, gold, test, dashboard)

### 15. Tests (`tests/`)
- [x] `test_episcope.py` — 20/20 test cases passing across:
  - 13 quality rules and constraint validation
  - Salted hash de-identification and age banding
  - Small-cell suppression
  - LP optimiser constraints (budget, non-negativity, equity floor)
  - Synthetic data generation determinism and overdispersion

### 16. Documentation
- [x] `README.md` — system architecture, quickstart, traceability table, design decisions
- [x] `docs/proposal.md` — M1 milestone proposal covering stakeholder conflicts (C1–C5)
- [x] `AI_USAGE.md` — AI tool usage declaration
- [x] `PROGRESS.md` — Comprehensive work progress tracking
- [x] `.env.example` & `.gitignore`

---

## 📊 Requirement Traceability

| Req ID | Status | File(s) | Implementation Notes |
|--------|--------|---------|----------------------|
| ING-1 | ✅ | `pipeline/bronze/ingest.py` | Ingests S1–S6 into bronze with source, batch_id, SHA-256 checksums |
| ING-2 | ✅ | All pipeline files | Fully implemented in Apache Spark (PySpark 4.2.0) |
| ING-3 | ✅ | `pipeline/bronze/ingest.py`, `pipeline/gold/aggregate.py` | Parquet storage with snappy compression, partitioned by epi_year |
| ING-4 | ✅ | `pipeline/bronze/ingest.py` | Idempotent load execution via overwrite partition mode |
| DQ-1 | ✅ | `pipeline/silver/transform.py` | 13 automated rules (schema, age, sex, dates, referential integrity, duplicates) |
| DQ-2 | ✅ | `pipeline/silver/transform.py` | JSON & Parquet data quality pass-rate reports (`dq_report.json`) |
| DQ-3 | ✅ | `pipeline/silver/transform.py` | Full batch_id lineage & quarantine table with reason codes |
| PRV-1 | ✅ | `pipeline/silver/transform.py` | Salted SHA-256 de-identification & 5-year age banding |
| PRV-2 | ✅ | `pipeline/gold/aggregate.py`, `serving/export.py` | Small-cell suppression (counts 1–4 → null) |
| AN-1 | ✅ | `analytics/detection/detect.py` | EARS C1/C2/C3 & CUSUM detection at scale via Spark window functions |
| AN-2 | ✅ | `analytics/detection/detect.py` | Outbreak benchmark evaluation (sensitivity, PPV, median delay) |
| AN-3 | ✅ | `analytics/detection/detect.py` | Threshold trade-off curve (Conflict C1: sensitivity vs specificity) |
| AN-4 | ✅ | `analytics/forecast/forecast.py` | 1–4 week-ahead forecasting per district with lagged climate covariates |
| AN-5 | ✅ | `analytics/forecast/forecast.py` | Rolling-origin 52-week backtesting against seasonal-naïve baseline |
| AN-6 | ✅ | `analytics/forecast/forecast.py` | Clinical resource demand conversion (beds, saline, NS1, ORS) |
| AN-7 | ✅ | `analytics/optimise/optimise.py` | Linear programming allocation optimiser with budget and equity floor |
| AN-8 | ✅ | `analytics/optimise/optimise.py` | Efficiency–equity frontier across 5 budget tiers and 5 α values |
| AN-9 | ✅ | `analytics/calendar/calendar.py` | Seasonal decomposition, risk classification & preparedness timing |
| VIS-1 | ✅ | `dashboard/index.html` | High-aesthetic D3.js surveillance dashboard |
| VIS-2 | ✅ | `dashboard/index.html` | 5 coordinated views (map, time series, heatmap, allocation, alerts) |
| VIS-3 | ✅ | `dashboard/index.html` | Bidirectional linked filters across district, disease, and year |
| VIS-4 | ✅ | `serving/export.py` | Strictly aggregated serving data (8.92 MB, well within 10MB limit) |
| VIS-6 | ✅ | `dashboard/index.html` | Color-blind safe palette, uncertainty intervals, data freshness |
| ENG-1 | ✅ | `run_all.py` / `Makefile` | One-command full pipeline execution with timing breakdown |
| ENG-2 | ✅ | `run_all.py` | Automated stage timing and scalability benchmarking report |
| ENG-3 | ✅ | `config.py`, all pipelines | Spark partitions, adaptive query execution, snappy parquet |
