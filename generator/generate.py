"""
EpiScope Synthetic Data Generator (S4–S6)
Generates deterministic, epidemiologically-realistic encounter records,
facility registry, and daily stock/bed-occupancy snapshots.

Specification compliance:
  - Deterministic for a given seed
  - SF=1 → ≥25M encounters across 64 districts, 2021-2025
  - Seasonality per disease, climate-driven incidence, overdispersion
  - 30+ injected outbreaks of varying size/duration/location
  - Reporting artefacts (facility silence + back-fill)
  - 2% duplicates, 0.5% orphan FKs, 1% invalid ages
"""

import os
import csv
import json
import hashlib
import uuid
import random
import math
from datetime import date, timedelta
from collections import defaultdict

import numpy as np

# Allow running from project root
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DEFAULT_SEED, DEFAULT_SF, START_YEAR, END_YEAR,
    DISEASE_CODES, BACKGROUND_CODES, DISEASE_PEAK_WEEK,
    DISEASE_BASE_INCIDENCE, DISEASE_SEVERITY,
    REPORTING_DELAY_MU, REPORTING_DELAY_SIGMA, MAX_REPORTING_DELAY,
    DUPLICATE_RATE, ORPHAN_FK_RATE, INVALID_AGE_RATE,
    DIVISIONS, DISTRICT_POPULATIONS, HILL_DISTRICTS, URBAN_DISTRICTS,
    RAW_DIR, get_all_districts, get_division,
)


# ────────────────────────────────────────────────────────────────────
# S5: Facility Registry
# ────────────────────────────────────────────────────────────────────
FACILITY_TYPES = [
    ("District Hospital",        250, 20),
    ("Upazila Health Complex",    50,  5),
    ("Union Sub-Centre",          10,  0),
    ("Private Clinic",            30,  3),
    ("Community Clinic",           5,  0),
]


def generate_facility_registry(rng, districts, output_path):
    """Generate ≥600 facilities across all districts (S5)."""
    facilities = []
    fid = 1
    for district in districts:
        pop = DISTRICT_POPULATIONS.get(district, 1500)
        # Roughly 10 facilities per million
        n_facilities = max(4, int(pop / 150))
        division = get_division(district)
        for _ in range(n_facilities):
            ftype, max_beds, max_icu = FACILITY_TYPES[rng.integers(0, len(FACILITY_TYPES))]
            beds = int(rng.integers(max(5, max_beds // 3), max(6, max_beds + 1)))
            icu_beds = int(rng.integers(0, max(1, max_icu + 1)))
            lat = 20.5 + rng.uniform(0, 6)  # rough BD bounding box
            lon = 88.0 + rng.uniform(0, 4)
            facilities.append({
                "facility_id": f"FAC-{fid:05d}",
                "facility_name": f"{district} {ftype} {fid}",
                "district": district,
                "division": division,
                "facility_type": ftype,
                "beds": beds,
                "icu_beds": icu_beds,
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
            })
            fid += 1

    # Ensure ≥ 600
    while len(facilities) < 600:
        district = districts[rng.integers(0, len(districts))]
        ftype, max_beds, max_icu = FACILITY_TYPES[rng.integers(0, len(FACILITY_TYPES))]
        beds = int(rng.integers(5, max(6, max_beds + 1)))
        icu_beds = int(rng.integers(0, max(1, max_icu + 1)))
        facilities.append({
            "facility_id": f"FAC-{fid:05d}",
            "facility_name": f"{district} {ftype} {fid}",
            "district": district,
            "division": get_division(district),
            "facility_type": ftype,
            "beds": beds,
            "icu_beds": icu_beds,
            "latitude": round(20.5 + rng.uniform(0, 6), 4),
            "longitude": round(88.0 + rng.uniform(0, 4), 4),
        })
        fid += 1

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=facilities[0].keys())
        writer.writeheader()
        writer.writerows(facilities)

    print(f"  ✓ Generated {len(facilities)} facilities → {output_path}")
    return facilities


# ────────────────────────────────────────────────────────────────────
# Outbreak injection
# ────────────────────────────────────────────────────────────────────
def generate_outbreak_schedule(rng, districts):
    """Generate 30+ outbreak events of varying size, duration, location."""
    outbreaks = []
    oid = 1

    diseases = ["dengue", "diarrhoea", "influenza", "malaria"]
    years = list(range(START_YEAR, END_YEAR + 1))

    # Distributed outbreaks across years and diseases
    for year in years:
        for _ in range(6):  # ~6 per year = 30 total
            disease = diseases[rng.integers(0, len(diseases))]
            district = districts[rng.integers(0, len(districts))]
            start_week = int(rng.integers(1, 49))
            duration_weeks = int(rng.integers(2, 12))
            # Multiplier over baseline (2x to 8x)
            multiplier = float(rng.uniform(2.0, 8.0))
            outbreaks.append({
                "outbreak_id": f"OB-{oid:03d}",
                "disease": disease,
                "district": district,
                "year": year,
                "start_week": start_week,
                "duration_weeks": duration_weeks,
                "peak_multiplier": round(multiplier, 2),
                "type": "localised",
            })
            oid += 1

    # One slow-growing outbreak (required)
    outbreaks.append({
        "outbreak_id": f"OB-{oid:03d}",
        "disease": "dengue",
        "district": "Dhaka",
        "year": 2024,
        "start_week": 20,
        "duration_weeks": 16,
        "peak_multiplier": 4.0,
        "type": "slow_growing",
    })
    oid += 1

    # One nationwide novel-respiratory surge (required)
    outbreaks.append({
        "outbreak_id": f"OB-{oid:03d}",
        "disease": "influenza",
        "district": "ALL",
        "year": 2023,
        "start_week": 10,
        "duration_weeks": 12,
        "peak_multiplier": 5.0,
        "type": "nationwide_respiratory",
    })
    oid += 1

    # Add a few more to ensure > 30
    for _ in range(3):
        disease = diseases[rng.integers(0, len(diseases))]
        district = districts[rng.integers(0, len(districts))]
        outbreaks.append({
            "outbreak_id": f"OB-{oid:03d}",
            "disease": disease,
            "district": district,
            "year": years[rng.integers(0, len(years))],
            "start_week": int(rng.integers(5, 45)),
            "duration_weeks": int(rng.integers(3, 10)),
            "peak_multiplier": round(float(rng.uniform(3.0, 6.0)), 2),
            "type": "localised",
        })
        oid += 1

    return outbreaks


# ────────────────────────────────────────────────────────────────────
# Reporting artefact: facility goes silent then back-fills
# ────────────────────────────────────────────────────────────────────
def generate_artefact_schedule(rng, facilities):
    """Generate at least 1 reporting artefact (facility silence + backfill)."""
    artefacts = []
    # Pick 2 facilities to have silence artefacts
    idxs = rng.choice(len(facilities), size=min(2, len(facilities)), replace=False)
    for idx in idxs:
        fac = facilities[idx]
        year = int(rng.integers(2022, 2025))
        start_week = int(rng.integers(10, 40))
        artefacts.append({
            "facility_id": fac["facility_id"],
            "district": fac["district"],
            "silent_year": year,
            "silent_start_week": start_week,
            "silent_weeks": 3,  # 3 weeks of silence
            "backfill": True,   # then back-fill all at once
        })
    return artefacts


# ────────────────────────────────────────────────────────────────────
# Helper: epi-week from date
# ────────────────────────────────────────────────────────────────────
def epi_week(d):
    """Return (epi_year, epi_week) using ISO week numbering."""
    iso = d.isocalendar()
    return iso[0], iso[1]


# ────────────────────────────────────────────────────────────────────
# S4: Encounter generator
# ────────────────────────────────────────────────────────────────────
def seasonal_factor(week, peak_week, concentration=3.0):
    """Von Mises-like seasonal factor peaking at peak_week."""
    theta = 2 * math.pi * (week - peak_week) / 52
    return math.exp(concentration * math.cos(theta)) / math.exp(concentration)


def district_disease_weight(district, disease):
    """Spatial weight: some diseases concentrate in certain districts."""
    if disease == "malaria":
        return 8.0 if district in HILL_DISTRICTS else 0.3
    elif disease == "dengue":
        return 3.0 if district in URBAN_DISTRICTS else 0.5
    elif disease == "diarrhoea":
        return 1.0  # widespread
    elif disease == "influenza":
        return 1.5 if district in URBAN_DISTRICTS else 0.8
    return 1.0


def get_outbreak_multiplier(district, disease, year, week, outbreaks):
    """Check if this district-disease-week is in an outbreak."""
    mult = 1.0
    for ob in outbreaks:
        if ob["disease"] != disease:
            continue
        if ob["year"] != year:
            continue
        if ob["district"] != "ALL" and ob["district"] != district:
            continue
        if ob["start_week"] <= week < ob["start_week"] + ob["duration_weeks"]:
            # Triangular profile: ramp up, peak, ramp down
            mid = ob["start_week"] + ob["duration_weeks"] / 2
            dist_from_mid = abs(week - mid) / (ob["duration_weeks"] / 2)
            local_mult = 1.0 + (ob["peak_multiplier"] - 1.0) * (1.0 - dist_from_mid)
            mult = max(mult, local_mult)
    return mult


def is_artefact_silent(facility_id, year, week, artefacts):
    """Check if a facility is in a silent period."""
    for art in artefacts:
        if art["facility_id"] == facility_id and art["silent_year"] == year:
            if art["silent_start_week"] <= week < art["silent_start_week"] + art["silent_weeks"]:
                return True
    return False


def generate_encounters(rng, sf, facilities, districts, outbreaks, artefacts, output_path):
    """
    Generate the health-facility encounter line list (S4).
    SF=1 → ≥25M records for 2021-2025 across 64 districts.
    Uses negative-binomial noise (overdispersion).
    """
    # Build facility lookup by district
    district_facilities = defaultdict(list)
    for fac in facilities:
        district_facilities[fac["district"]].append(fac["facility_id"])

    all_facility_ids = [f["facility_id"] for f in facilities]
    diseases = list(DISEASE_CODES.keys())
    all_icd10 = []
    for codes in DISEASE_CODES.values():
        all_icd10.extend(codes)

    # Pre-compute date range
    start_date = date(START_YEAR, 1, 1)
    end_date = date(END_YEAR, 12, 31)
    total_days = (end_date - start_date).days + 1

    # Base encounters per district per year at SF=1 ≈ 25M / 64 / 5 ≈ ~78K
    base_per_district_per_year = int(78000 * sf)

    records = []
    encounter_id_counter = 0
    total_target = int(25_000_000 * sf)

    print(f"  Generating encounters (SF={sf}, target ≈ {total_target:,}) ...")

    for year in range(START_YEAR, END_YEAR + 1):
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)

        for district in districts:
            pop = DISTRICT_POPULATIONS.get(district, 1500) * 1000  # in persons
            facs_in_district = district_facilities.get(district, all_facility_ids[:3])

            for disease in diseases:
                base_inc = DISEASE_BASE_INCIDENCE[disease]
                spatial_w = district_disease_weight(district, disease)
                sev = DISEASE_SEVERITY[disease]
                icd10_codes = DISEASE_CODES[disease]

                for week in range(1, 53):
                    # Seasonal component
                    s_factor = seasonal_factor(week, DISEASE_PEAK_WEEK[disease])

                    # Outbreak multiplier
                    ob_mult = get_outbreak_multiplier(district, disease, year, week, outbreaks)

                    # Expected weekly count
                    lam = (base_inc / 100000) * pop * sf * spatial_w * s_factor * ob_mult / 52

                    # Negative-binomial draw (overdispersion)
                    r_param = max(1.0, lam * 0.5)  # dispersion
                    p_param = r_param / (r_param + max(lam, 0.01))
                    n_cases = int(rng.negative_binomial(r_param, p_param))
                    n_cases = min(n_cases, 5000)  # cap per week

                    # Generate encounters for this week
                    week_start = year_start + timedelta(weeks=week - 1)

                    for _ in range(n_cases):
                        encounter_id_counter += 1
                        eid = f"ENC-{encounter_id_counter:010d}"

                        # Onset date within the week
                        day_offset = int(rng.integers(0, 7))
                        onset_date = week_start + timedelta(days=day_offset)
                        if onset_date > year_end:
                            onset_date = year_end

                        # Reporting delay (lognormal, right-skewed)
                        delay = int(min(
                            rng.lognormal(REPORTING_DELAY_MU, REPORTING_DELAY_SIGMA),
                            MAX_REPORTING_DELAY
                        ))
                        # Fewer reports on weekends
                        report_date = onset_date + timedelta(days=delay)
                        if report_date.weekday() >= 5:  # Sat/Sun
                            report_date += timedelta(days=(7 - report_date.weekday()))

                        # Patient demographics
                        age = int(rng.exponential(25) + 1)
                        age = min(age, 95)
                        sex = "M" if rng.random() < 0.52 else "F"

                        # Invalid age injection (1%)
                        if rng.random() < INVALID_AGE_RATE:
                            age = int(rng.choice([212, -5, 999, 0]))

                        # Facility assignment
                        fac_id = facs_in_district[rng.integers(0, len(facs_in_district))]

                        # Orphan FK injection (0.5%)
                        if rng.random() < ORPHAN_FK_RATE:
                            fac_id = f"FAC-{rng.integers(90000, 99999):05d}"

                        # Check artefact (facility silence)
                        ey, ew = epi_week(onset_date)
                        if is_artefact_silent(fac_id, ey, ew, artefacts):
                            # Backfill: set report_date to after silence
                            art_match = [a for a in artefacts if a["facility_id"] == fac_id and a["silent_year"] == ey]
                            if art_match:
                                backfill_week = art_match[0]["silent_start_week"] + art_match[0]["silent_weeks"] + 1
                                backfill_date = year_start + timedelta(weeks=backfill_week - 1)
                                report_date = backfill_date + timedelta(days=int(rng.integers(0, 3)))

                        # ICD-10
                        icd10 = icd10_codes[rng.integers(0, len(icd10_codes))]

                        # Severity cascade
                        lab_confirmed = bool(rng.random() < 0.6)
                        admitted = bool(rng.random() < sev["admit_rate"] * ob_mult)
                        if admitted:
                            outcome = "deceased" if rng.random() < sev["cfr"] * ob_mult else "discharged"
                        else:
                            outcome = "outpatient"

                        # Patient ID (synthetic national ID)
                        patient_id = f"PID-{rng.integers(1, int(pop)):010d}"

                        records.append({
                            "encounter_id": eid,
                            "patient_id": patient_id,
                            "facility_id": fac_id,
                            "onset_date": onset_date.isoformat(),
                            "report_date": report_date.isoformat(),
                            "age_years": age,
                            "sex": sex,
                            "icd10_code": icd10,
                            "lab_confirmed": lab_confirmed,
                            "admitted": admitted,
                            "outcome": outcome,
                        })

        print(f"    Year {year}: {len(records):,} cumulative records")

    # Add background disease encounters
    n_background = int(len(records) * 0.15)  # 15% background
    print(f"  Adding {n_background:,} background encounters ...")
    for i in range(n_background):
        encounter_id_counter += 1
        day_idx = int(rng.integers(0, total_days))
        onset = start_date + timedelta(days=day_idx)
        delay = int(min(rng.lognormal(REPORTING_DELAY_MU, REPORTING_DELAY_SIGMA), MAX_REPORTING_DELAY))
        report = onset + timedelta(days=delay)
        district = districts[rng.integers(0, len(districts))]
        facs = district_facilities.get(district, all_facility_ids[:3])
        records.append({
            "encounter_id": f"ENC-{encounter_id_counter:010d}",
            "patient_id": f"PID-{rng.integers(1, 10000000):010d}",
            "facility_id": facs[rng.integers(0, len(facs))],
            "onset_date": onset.isoformat(),
            "report_date": report.isoformat(),
            "age_years": int(rng.exponential(30) + 1),
            "sex": "M" if rng.random() < 0.52 else "F",
            "icd10_code": BACKGROUND_CODES[rng.integers(0, len(BACKGROUND_CODES))],
            "lab_confirmed": False,
            "admitted": bool(rng.random() < 0.05),
            "outcome": "outpatient",
        })

    # Inject duplicates (2%)
    n_dupes = int(len(records) * DUPLICATE_RATE)
    print(f"  Injecting {n_dupes:,} duplicate records ...")
    dupe_indices = rng.choice(len(records), size=n_dupes, replace=True)
    for idx in dupe_indices:
        records.append(records[idx].copy())  # exact duplicate

    # Shuffle
    rng.shuffle(records)

    # Write CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    print(f"  ✓ Generated {len(records):,} encounter records → {output_path}")
    return len(records)


# ────────────────────────────────────────────────────────────────────
# S6: Daily stock and bed-occupancy snapshots
# ────────────────────────────────────────────────────────────────────
def generate_stock_snapshots(rng, facilities, output_path):
    """Generate daily stock and bed-occupancy snapshots (S6)."""
    snapshots = []
    start = date(START_YEAR, 1, 1)
    end = date(END_YEAR, 12, 31)
    total_days = (end - start).days + 1

    # Sample every 7 days to keep size manageable, then interpolate
    sample_days = list(range(0, total_days, 1))  # daily
    # For large scale, sample weekly
    if len(facilities) * total_days > 2_000_000:
        sample_days = list(range(0, total_days, 7))

    for fac in facilities:
        beds = fac["beds"]
        for day_idx in sample_days:
            snap_date = start + timedelta(days=day_idx)
            # Occupancy varies seasonally
            _, week = epi_week(snap_date)
            occ_rate = 0.5 + 0.3 * seasonal_factor(week, 35)
            occupied = int(min(beds, max(0, rng.binomial(beds, occ_rate))))

            snapshots.append({
                "facility_id": fac["facility_id"],
                "snapshot_date": snap_date.isoformat(),
                "iv_saline_units": int(rng.integers(10, 500)),
                "ns1_test_kits": int(rng.integers(0, 200)),
                "ors_packets": int(rng.integers(50, 2000)),
                "antimalarials_doses": int(rng.integers(0, 300)),
                "occupied_beds": occupied,
                "total_beds": beds,
            })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write as JSON lines (incremental feed format)
    with open(output_path, "w", encoding="utf-8") as f:
        for snap in snapshots:
            f.write(json.dumps(snap) + "\n")

    print(f"  ✓ Generated {len(snapshots):,} stock snapshots → {output_path}")
    return snapshots


# ────────────────────────────────────────────────────────────────────
# Main entry
# ────────────────────────────────────────────────────────────────────
def run_generator(sf=None, seed=None):
    """Run the full synthetic data generator."""
    if sf is None:
        sf = float(os.environ.get("EPISCOPE_SF", DEFAULT_SF))
    if seed is None:
        seed = int(os.environ.get("EPISCOPE_SEED", DEFAULT_SEED))

    print(f"═══ EpiScope Generator ═══")
    print(f"  Scale Factor: {sf}")
    print(f"  Seed: {seed}")
    print(f"  Date range: {START_YEAR}-{END_YEAR}")

    rng = np.random.default_rng(seed)
    districts = get_all_districts()

    # S5: Facility registry
    fac_path = os.path.join(RAW_DIR, "facility_registry.csv")
    facilities = generate_facility_registry(rng, districts, fac_path)

    # Outbreak schedule (ground truth — never used by detection)
    outbreaks = generate_outbreak_schedule(rng, districts)
    gt_path = os.path.join(RAW_DIR, "ground_truth_outbreaks.csv")
    os.makedirs(os.path.dirname(gt_path), exist_ok=True)
    with open(gt_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=outbreaks[0].keys())
        writer.writeheader()
        writer.writerows(outbreaks)
    print(f"  ✓ Generated {len(outbreaks)} outbreak events → {gt_path}")

    # Artefact schedule
    artefacts = generate_artefact_schedule(rng, facilities)
    art_path = os.path.join(RAW_DIR, "artefact_schedule.json")
    with open(art_path, "w", encoding="utf-8") as f:
        json.dump(artefacts, f, indent=2)
    print(f"  ✓ Generated {len(artefacts)} reporting artefacts → {art_path}")

    # S4: Encounter line list
    enc_path = os.path.join(RAW_DIR, "encounters.csv")
    n_records = generate_encounters(rng, sf, facilities, districts, outbreaks, artefacts, enc_path)

    # S6: Stock snapshots
    stock_path = os.path.join(RAW_DIR, "stock_snapshots.jsonl")
    generate_stock_snapshots(rng, facilities, stock_path)

    print(f"\n═══ Generation Complete ═══")
    print(f"  Total encounters: {n_records:,}")
    print(f"  Outbreaks: {len(outbreaks)}")
    print(f"  Facilities: {len(facilities)}")
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="EpiScope Synthetic Data Generator")
    parser.add_argument("--sf", type=float, default=DEFAULT_SF, help="Scale factor (0.25, 0.5, 1.0)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed")
    args = parser.parse_args()
    run_generator(sf=args.sf, seed=args.seed)
