# AI Tool Usage Declaration

> **Course**: CSE 4345 – Big Data Analytics (Spring 2026)  
> **Project**: EpiScope — Big Data Surveillance, Forecasting & Response-Planning Platform  
> **Institution**: Department of Computer Science & Engineering, Premier University, Chittagong

---

## 1. Tool Declaration

- **Tool Used**: Antigravity AI Coding Assistant / Pair Programmer
- **Underlying Models**: Claude 3.7 Sonnet / Claude Opus 4.6 (Thinking) / Gemini 3.8 Flash
- **Primary Use**: Architecture design consultation, boilerplate synthesis, PySpark pipeline generation, LP mathematical formulation in PuLP, and D3.js visualization structure.

---

## 2. Prompts and Intent Log

| Date & Session | Intent / Task | Prompt Summary | AI Output Utilised | Human Review & Modifications |
|---|---|---|---|---|
| Phase 1 | Project Scaffolding & Spec Analysis | "Analyze CEP_EpiScope.pdf and establish project directory layout according to Appendix A." | Project structure, requirements mapping, configuration scaffolding (`config.py`). | Verified all 64 Bangladesh districts, centroids, populations, and ICD-10 disease mappings. |
| Phase 2 | Synthetic Data & Climate Acquisition | "Create synthetic data generator for encounters, facilities, daily stocks, and Open-Meteo weather integration." | Synthetic generator scripts (`generate.py`, `fetch_weather.py`) implementing Negative-Binomial overdispersion and seasonal profiles. | Added Open-Meteo caching, synthetic weather fallback, and resolved district coordinate collisions. |
| Phase 3 | Lakehouse Engineering (Bronze/Silver/Gold) | "Build Medallion Lakehouse with PySpark: Bronze ingestion, Silver 13 DQ rules + de-identification, Gold marts + small-cell suppression." | PySpark transformation jobs (`ingest.py`, `transform.py`, `aggregate.py`), SHA-256 lineage, salted hashing, and 5-year age banding. | Added small-cell privacy masking (counts 1–4 → null), quarantine tables, and memory/partition optimizations. |
| Phase 4 | Analytics & Optimization | "Implement EARS C1/C2/C3 + CUSUM detection, rolling regression forecasting with weather lags, and LP resource allocation." | Analytics algorithms (`detect.py`, `forecast.py`, `optimise.py`, `calendar.py`). | Fixed standard library `calendar` module shadowing; added pandas fallback for `applyInPandas` when PyArrow mismatch occurred. |
| Phase 5 | Serving & Visualisation | "Develop D3.js v7 interactive dashboard with 5 coordinated views and export compact serving JSON under 10MB." | Serving export script (`export.py`), D3 dashboard (`index.html`) with choropleth, timeline, heatmap, and allocation charts. | Optimized float rounding to compress serving data from 10.3 MB to 8.92 MB (complying with VIS-4 limit); replaced undefined `d3.debounce` with custom debounce function. |
| Phase 6 | Orchestration & Automated Testing | "Assemble one-command run_all.py pipeline orchestrator and pytest test suite." | `run_all.py` timing benchmarks, Makefile, and 20 unit tests in `tests/test_episcope.py`. | Verified 20/20 tests passing and validated end-to-end execution at Scale Factor 0.25. |

---

## 3. Human Critical Evaluation & Debugging Log

All AI-suggested code was critically evaluated, debugged, and validated against the course specification:

1. **Java 21 / PySpark 4.2.0 Compatibility**:
   - *Problem*: PySpark 4.2.0 failed on Windows with `Module jdk.incubator.vector not found` due to PATH pointing to legacy Java 8 or stripped IDE runtimes.
   - *Action*: Audited system JDK installations, targeted Eclipse Adoptium OpenJDK 21, and automated environment variable configuration in `config.py`.

2. **Windows Character Encoding**:
   - *Problem*: Box-drawing and status glyphs threw `UnicodeEncodeError` in Windows console (`cp1252`).
   - *Action*: Configured `sys.stdout` and `sys.stderr` UTF-8 reconfiguring across all entrypoints.

3. **Standard Library Namespace Collision**:
   - *Problem*: `analytics/calendar/calendar.py` shadowed Python's built-in `calendar` module, breaking date parsing in Pandas/`_strptime`.
   - *Action*: Sanitized `sys.path` dynamically before loading standard library dependencies.

4. **Serving Layer Constraint Enforcement (VIS-4)**:
   - *Problem*: Raw JSON serialization of timeseries and alert records generated 10.31 MB of data, violating the 10 MB maximum payload rule.
   - *Action*: Enforced 2-decimal float precision and stripped whitespace with compact JSON separators (`separators=(',', ':')`), reducing the total payload to **8.92 MB**.

5. **D3.js Frontend Runtime Bug**:
   - *Problem*: `index.html` threw `TypeError: d3.debounce is not a function` during window resize.
   - *Action*: Implemented a native JavaScript debounce utility function and verified that all 5 views render cleanly without console warnings.
