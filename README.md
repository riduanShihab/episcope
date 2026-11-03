# EpiScope — Big Data Surveillance, Forecasting & Response-Planning Platform

> **Course**: CSE 4345 – Big Data Analytics (Comprehensive Engineering Project)  
> **Target Region**: All 64 Districts across 8 Administrative Divisions of Bangladesh  
> **Technologies**: Apache Spark (PySpark 4.2.0), Python 3.11, OpenJDK 21, PuLP/CBC, D3.js v7, Parquet

---

## 📋 Executive Overview

**EpiScope** is a Big Data surveillance and decision-support platform designed for the Ministry of Health (MoH). It ingests health encounters, facility operational registries, climate time series, and medical inventory snapshots to detect outbreaks, forecast disease surges, and optimize the distribution of critical medical supplies under budgetary and regional equity constraints.

### Targeted Conditions & ICD-10 Codes
- **Dengue Fever**: `A90`, `A91` (Vector-borne, rainfall/temperature lag drivers)
- **Diarrhoeal Disease / Cholera**: `A00`, `A09` (Water-borne, extreme precipitation drivers)
- **Influenza-Like Illness (ILI)**: `J10`, `J11` (Airborne respiratory, winter seasonality)
- **Malaria**: `B50`–`B54` (Endemic border districts)

---

## 🏗️ Architecture & Medallion Pipeline

EpiScope implements a structured Lakehouse architecture using Apache Spark:

```
[Raw Sources: S1-S6]
  │  (CSV, JSONL, Open-Meteo REST API)
  ▼
[Bronze Layer] ──► Parquet format, Snappy compression, partitioned by epi_year.
  │                Ingestion lineage: batch_id, file checksums (SHA-256), timestamps.
  ▼
[Silver Layer] ──► 13 Automated Data Quality rules, quarantine tables with reason codes.
  │                De-identification: Salted SHA-256 patient hashing, 5-year age banding.
  ▼
[Gold Layer]   ──► Epi-week aggregate marts with small-cell privacy suppression (<5 → null).
  │                4/6/8-week climate lags, stock summaries, and unified feature tables.
  ▼
[Analytics]    ──► 1. Outbreak Detection: EARS C1/C2/C3 & CUSUM window algorithms.
  │                2. Short-term Forecasting: Multi-covariate regression vs seasonal-naïve.
  │                3. Allocation Optimiser: PuLP LP solver with equity floor constraints.
  │                4. Preparedness Calendar: Seasonal decomposition & intervention timing.
  ▼
[Serving Layer]──► Aggregated JSON export (8.92 MB <= 10MB limit) serving D3.js v7 UI.
```

---

## 🚀 Quickstart & One-Command Run

### 1. Prerequisites
- **Python**: 3.11+
- **Java**: OpenJDK 17 or 21 (Eclipse Adoptium recommended)
- **Node.js**: v18+ (optional, for dashboard validation)

Install Python dependencies:
```bash
pip install -r requirements.txt
```

### 2. Full End-to-End Pipeline Execution (ENG-1)
Run the entire platform from data ingestion through analytical modeling and dashboard export with a single command:

```bash
# Development mode (Scale Factor 0.25)
python run_all.py --sf 0.25

# Re-run pipeline skipping raw data generation
python run_all.py --sf 0.25 --skip-generate
```

### 3. Launch the Interactive Dashboard
To view the 5 coordinated views in your browser:

```bash
cd dashboard
python -m http.server 8000 --bind 127.0.0.1
```
Open **[http://127.0.0.1:8000](http://127.0.0.1:8000)** in any modern browser.

---

## 🧪 Automated Testing

Run the automated test suite covering all quality checks, hashing privacy, and LP constraints:

```bash
python -m pytest tests/test_episcope.py -v
```
*(All 20 unit and integration tests passing)*

---

## 📊 Requirement Traceability Matrix

| Requirement | Description | Status | Implementation Reference |
|---|---|:---:|---|
| **ING-1** | Ingest S1–S6 raw sources with metadata (source, batch_id, checksum) | ✅ | `pipeline/bronze/ingest.py` |
| **ING-2** | Apache Spark utilized for all transformation pipelines | ✅ | All jobs in `pipeline/` and `analytics/` |
| **ING-3** | Parquet format with Snappy compression, partitioned by year | ✅ | `pipeline/bronze/ingest.py`, `pipeline/gold/aggregate.py` |
| **ING-4** | Idempotent execution (safe re-run without duplicate accumulation) | ✅ | Overwrite partition modes in all pipeline stages |
| **DQ-1** | 13 automated data quality rules (schema, range, referential integrity) | ✅ | `pipeline/silver/transform.py` |
| **DQ-2** | Per-load Data Quality report with pass-rate percentages | ✅ | `data/silver/dq_report.json`, `data/gold/dq_report` |
| **DQ-3** | Quarantine table preserving bad records with reason codes and lineage | ✅ | `data/silver/quarantine` |
| **PRV-1** | Salted SHA-256 patient de-identification and 5-year age banding | ✅ | `pipeline/silver/transform.py` |
| **PRV-2** | Small-cell privacy suppression (case counts 1–4 suppressed to null) | ✅ | `pipeline/gold/aggregate.py`, `serving/export.py` |
| **AN-1** | EARS C1/C2/C3 and CUSUM outbreak detection algorithms | ✅ | `analytics/detection/detect.py` |
| **AN-2** | Outbreak benchmark evaluation (sensitivity, PPV, detection latency) | ✅ | `data/gold/detection_evaluation.json` |
| **AN-3** | Sensitivity vs. specificity threshold trade-off curve (Conflict C1) | ✅ | `data/gold/threshold_tradeoff.json` |
| **AN-4** | 1–4 week-ahead forecasts with lagged weather covariates | ✅ | `analytics/forecast/forecast.py` |
| **AN-5** | Rolling-origin 52-week backtesting against seasonal-naïve baseline | ✅ | `data/gold/forecast_evaluation.json` |
| **AN-6** | Translation of case forecasts into beds and medical consumable demands | ✅ | `analytics/forecast/forecast.py` |
| **AN-7** | Linear programming allocation optimiser with equity floors | ✅ | `analytics/optimise/optimise.py` |
| **AN-8** | Efficiency–equity trade-off frontier across budget tiers (Conflict C3) | ✅ | `data/gold/efficiency_equity_curve.json` |
| **AN-9** | Seasonal preparedness calendar with supply and campaign lead times | ✅ | `analytics/calendar/calendar.py` |
| **VIS-1** | Coordinated interactive surveillance dashboard | ✅ | `dashboard/index.html` |
| **VIS-2** | 5 synchronized views: map, timeline, heatmap, allocation, alert table | ✅ | `dashboard/index.html` |
| **VIS-3** | Bidirectional interactive filtering (district, year, disease) | ✅ | `dashboard/index.html` |
| **VIS-4** | Strict serving layer payload constraint ($\le 10\text{ MB}$) | ✅ | `serving/export.py` (Current size: 8.92 MB) |
| **VIS-6** | Accessible color palettes, prediction intervals, freshness indicators | ✅ | `dashboard/index.html` |
| **ENG-1** | One-command execution master script | ✅ | `run_all.py`, `Makefile` |
| **ENG-2** | Scalability and execution timing benchmarking report | ✅ | `data/serving/timing_report.json` |
| **ENG-3** | Spark performance tuning (adaptive execution, shuffle partitions) | ✅ | `config.py`, Spark configs |

---

## ⚖️ Trade-off & Conflict Analysis (C1–C5)

- **C1: Sensitivity vs. Specificity**: Tuned detection to EARS C2 ($z=2.0$) minimizing false alarms during background non-epidemic periods while achieving prompt detection latency ($\le 1$ week) during verified outbreaks.
- **C2: Granularity vs. Privacy**: Retained district-level administrative reporting granularity while strictly applying salted cryptographic hashes and suppressing small cell counts ($1 \le n \le 4$).
- **C3: Efficiency vs. Equity**: Implemented an equity floor parameter ($\alpha = 0.1$) within the LP solver, ensuring even low-incidence rural districts maintain minimum consumable allocations.
- **C4: Freshness vs. Cost**: Scheduled daily batch rollups into Snappy Parquet, achieving instant query latency for serving layers while keeping execution cost low.
- **C5: Accuracy vs. Interpretability**: Selected multi-covariate linear regression with lagged climate terms, providing transparent coefficients that epidemiological teams can validate against historical seasonal drivers.
