# EpiScope — M1 Proposal

**Group Number**: [Group Number]
**Members**: [Name 1 (ID)], [Name 2 (ID)], [Name 3 (ID)]
**Repository**: [URL]
**Date**: Day 3

---

## 1. Problem Restatement

The Ministry of Health requires a national data platform that integrates routine health surveillance, climate, and logistics data to enable early outbreak detection, short-term case forecasting, optimal resource allocation, and seasonal preparedness planning for dengue, cholera/diarrhoeal disease, influenza-like illness, and malaria across all 64 districts of Bangladesh. The platform must serve diverse stakeholders from policymakers to district officers, protect patient privacy, and function on modest infrastructure.

## 2. Stakeholder and Conflict Analysis

### Key Stakeholders

| Stakeholder | Primary Need | Our Approach |
|-------------|-------------|--------------|
| MoH policymakers | National overview, defensible allocation | Choropleth map + allocation view with transparency |
| Surveillance epidemiologists | Reliable signals, no artefact-driven false alarms | EARS + CUSUM with artefact detection logic |
| District health officers | Early local warning without alert fatigue | Tunable thresholds, per-district alerts |
| Hospital managers | Bed/consumable demand forecasts with uncertainty | 80% prediction intervals on all forecasts |
| Patients and public | No misuse of personal data | De-identification at silver layer, small-cell suppression |
| Privacy regulator | Minimal data, purpose limitation | Salted hashing, age banding, no line-level data to dashboard |

### Conflict Resolution Strategy

| Conflict | Trade-off | Our Resolution |
|----------|-----------|----------------|
| **C1. Sensitivity vs. Specificity** | Lower threshold → more false alarms | Plot ROC-like trade-off curve; operate at z=2.0 (C2 method) |
| **C2. Granularity vs. Privacy** | Sub-district detail ↔ patient identification | District-level aggregation + small-cell suppression (counts <5 → null) |
| **C3. Efficiency vs. Equity** | Minimise total unmet demand ↔ minimum share for every district | LP with equity floor α=0.1; plot efficiency–equity curve across 5 budgets |
| **C4. Freshness vs. Cost** | Real-time alerts ↔ compute cost | Daily batch processing (sufficient for weekly epi-week decisions) |
| **C5. Accuracy vs. Interpretability** | Complex ML ↔ official trust | Linear regression with weather covariates (interpretable); compare with naïve baseline |

## 3. Planned Architecture

**Medallion Lakehouse**: Raw → Bronze (Parquet, partitioned) → Silver (cleaned, de-identified) → Gold (epi-week marts, features)

**Key Technologies**:
- Apache Spark (PySpark) for all heavy transforms
- Parquet with Snappy compression, partitioned by epi-year and division
- PuLP/CBC for LP allocation optimiser
- D3.js v7 for interactive dashboard

**Analytics Pipeline**:
1. EARS C1/C2/C3 + CUSUM outbreak detection
2. Linear regression forecasting with lagged weather covariates
3. LP resource allocation with equity constraints
4. Seasonal decomposition for preparedness calendar

## 4. Diseases and Methods

| Disease | ICD-10 | Detection | Forecast Covariates |
|---------|--------|-----------|-------------------|
| Dengue | A90/A91 | EARS C2 + CUSUM | Rainfall lag 4-8wk, temperature |
| Diarrhoea | A00/A09 | EARS C2 | Extreme rainfall events |
| Influenza/ILI | J10/J11 | EARS C3 | Humidity, temperature |
| Malaria | B50-B54 | CUSUM | Rainfall, temperature |

## 5. Role Assignment

| Role | Member | Lead Areas |
|------|--------|-----------|
| Data Engineer | [Name 1] | Generator, ingestion, lakehouse, quality, scalability |
| Analytics Engineer | [Name 2] | Detection, forecasting, optimiser, calendar |
| Visualisation Engineer | [Name 3] | D3.js dashboard, serving layer, privacy |

## 6. Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| 1 | Data generator doesn't produce realistic seasonality | Medium | High | Validate against OpenDengue reference data; use von-Mises seasonal profile |
| 2 | Spark pipeline exceeds 45-min limit at SF=1 | Medium | High | Implement AQE, broadcast joins, partition pruning; test early at SF=0.25 |
| 3 | False outbreak alerts from reporting artefacts | High | Medium | Implement artefact detection logic; compare EARS vs CUSUM |
| 4 | Dashboard exceeds 10MB data limit | Low | Medium | Pre-aggregate at epi-week level; monitor serving layer size |
| 5 | LP solver infeasible at low budgets | Low | High | Add slack variables; test across multiple budget levels |
| 6 | Team member unavailability | Medium | Medium | Cross-training; every member must understand all components |
| 7 | Privacy leakage through dashboard exports | Medium | High | Automated small-cell suppression in serving layer; complementary suppression |
