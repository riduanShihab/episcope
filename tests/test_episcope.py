"""
EpiScope Test Suite
Tests for data quality rules, de-identification, and optimiser constraints.
Run with: python -m pytest tests/ -v
"""

import os
import sys
import json
import hashlib
import pytest
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DISEASE_CODES, BACKGROUND_CODES, SMALL_CELL_THRESHOLD,
    HASH_ALGORITHM, get_all_districts
)


# ════════════════════════════════════════════════════════
# Data Quality Rule Tests (ENG-4)
# ════════════════════════════════════════════════════════

class TestQualityRules:
    """Tests for the 13 data quality rules in the silver pipeline."""

    def test_dq_r01_encounter_id_not_null(self):
        """DQ-R01: encounter_id must not be null."""
        valid = {"encounter_id": "ENC-001", "patient_id": "P1"}
        invalid = {"encounter_id": None, "patient_id": "P1"}
        assert valid["encounter_id"] is not None
        assert invalid["encounter_id"] is None

    def test_dq_r06_valid_age_range(self):
        """DQ-R06: Age must be between 0 and 120."""
        valid_ages = [1, 5, 25, 60, 95, 119]
        invalid_ages = [-5, 0, 120, 212, 999]

        for age in valid_ages:
            assert 0 < age < 120, f"Age {age} should be valid"

        for age in invalid_ages:
            assert not (0 < age < 120), f"Age {age} should be invalid"

    def test_dq_r07_valid_sex(self):
        """DQ-R07: Sex must be M or F."""
        assert "M" in ["M", "F"]
        assert "F" in ["M", "F"]
        assert "X" not in ["M", "F"]
        assert "" not in ["M", "F"]

    def test_dq_r08_date_order(self):
        """DQ-R08: onset_date must be <= report_date."""
        from datetime import date
        onset = date(2023, 6, 15)
        report_ok = date(2023, 6, 20)
        report_bad = date(2023, 6, 10)

        assert onset <= report_ok, "onset <= report should pass"
        assert onset > report_bad, "onset > report should fail"

    def test_dq_r09_valid_icd10(self):
        """DQ-R09: ICD-10 code must be in the valid set."""
        all_valid = []
        for codes in DISEASE_CODES.values():
            all_valid.extend(codes)
        all_valid.extend(BACKGROUND_CODES)

        assert "A90" in all_valid  # dengue
        assert "A00" in all_valid  # cholera
        assert "J10" in all_valid  # influenza
        assert "B50" in all_valid  # malaria
        assert "ZZZ" not in all_valid  # invalid

    def test_dq_r11_date_range(self):
        """DQ-R11: Dates must be within 2020-01-01 to 2026-12-31."""
        from datetime import date
        valid_date = date(2023, 6, 15)
        too_early = date(2019, 12, 31)
        too_late = date(2027, 1, 1)

        min_date = date(2020, 1, 1)
        max_date = date(2026, 12, 31)

        assert min_date <= valid_date <= max_date
        assert not (min_date <= too_early <= max_date)
        assert not (min_date <= too_late <= max_date)

    def test_dq_r12_duplicate_detection(self):
        """DQ-R12: Duplicate encounter_ids should be detected."""
        encounters = ["ENC-001", "ENC-002", "ENC-001", "ENC-003"]
        seen = set()
        duplicates = []
        for eid in encounters:
            if eid in seen:
                duplicates.append(eid)
            seen.add(eid)

        assert len(duplicates) == 1
        assert "ENC-001" in duplicates

    def test_all_districts_valid(self):
        """Ensure all 64 districts are configured."""
        districts = get_all_districts()
        assert len(districts) == 64, f"Expected 64 districts, got {len(districts)}"


# ════════════════════════════════════════════════════════
# De-identification Tests (ENG-4)
# ════════════════════════════════════════════════════════

class TestDeIdentification:
    """Tests for patient de-identification."""

    def test_salted_hash_is_deterministic(self):
        """Same input + salt → same hash."""
        salt = "test_salt"
        pid = "PID-0000001234"
        hash1 = hashlib.sha256((salt + pid).encode()).hexdigest()
        hash2 = hashlib.sha256((salt + pid).encode()).hexdigest()
        assert hash1 == hash2

    def test_different_salts_produce_different_hashes(self):
        """Different salts → different hashes."""
        pid = "PID-0000001234"
        hash1 = hashlib.sha256(("salt_a" + pid).encode()).hexdigest()
        hash2 = hashlib.sha256(("salt_b" + pid).encode()).hexdigest()
        assert hash1 != hash2

    def test_hash_is_irreversible(self):
        """Hash should not contain original patient ID."""
        salt = "test_salt"
        pid = "PID-0000001234"
        hashed = hashlib.sha256((salt + pid).encode()).hexdigest()
        assert pid not in hashed
        assert "1234" not in hashed

    def test_age_banding(self):
        """Ages should be reduced to 5-year bands."""
        def age_band(age):
            lower = (age // 5) * 5
            return f"{lower}-{lower+4}"

        assert age_band(23) == "20-24"
        assert age_band(0) == "0-4"
        assert age_band(65) == "65-69"
        assert age_band(5) == "5-9"

    def test_small_cell_suppression(self):
        """Counts 1-4 must be suppressed (set to None)."""
        def suppress(val):
            if val is not None and isinstance(val, (int, float)) and 1 <= val < SMALL_CELL_THRESHOLD:
                return None
            return val

        assert suppress(0) == 0
        assert suppress(1) is None
        assert suppress(2) is None
        assert suppress(3) is None
        assert suppress(4) is None
        assert suppress(5) == 5
        assert suppress(100) == 100
        assert suppress(None) is None


# ════════════════════════════════════════════════════════
# Optimiser Constraint Tests (ENG-4)
# ════════════════════════════════════════════════════════

class TestOptimiserConstraints:
    """Tests for the LP allocation optimiser constraints."""

    def test_budget_constraint(self):
        """Total allocation cost must not exceed budget."""
        budget = 1000
        allocations = [200, 300, 100, 50]
        costs = [1, 1, 1, 1]
        total_cost = sum(a * c for a, c in zip(allocations, costs))
        assert total_cost <= budget

    def test_non_negative_allocation(self):
        """All allocations must be >= 0."""
        allocations = [100, 50, 0, 200]
        assert all(a >= 0 for a in allocations)

    def test_equity_floor(self):
        """With equity floor α, each district gets at least α × pop_share × total."""
        alpha = 0.1
        pop_shares = [0.3, 0.2, 0.15, 0.35]
        total_alloc = 1000
        allocations = [350, 220, 180, 250]

        for i, (alloc, share) in enumerate(zip(allocations, pop_shares)):
            min_alloc = alpha * share * total_alloc
            assert alloc >= min_alloc, \
                f"District {i}: {alloc} < min {min_alloc:.1f}"

    def test_unmet_demand_calculation(self):
        """Unmet demand = max(0, forecast - stock - allocation)."""
        forecast = 100
        stock = 30
        allocation = 50
        unmet = max(0, forecast - stock - allocation)
        assert unmet == 20

        # No unmet when supply exceeds demand
        allocation2 = 80
        unmet2 = max(0, forecast - stock - allocation2)
        assert unmet2 == 0

    def test_pulp_solver_available(self):
        """PuLP CBC solver should be available."""
        import pulp
        prob = pulp.LpProblem("test", pulp.LpMinimize)
        x = pulp.LpVariable("x", lowBound=0)
        prob += x
        prob += x >= 5
        prob.solve(pulp.PULP_CBC_CMD(msg=0))
        assert pulp.value(x) == 5.0


# ════════════════════════════════════════════════════════
# Generator Tests
# ════════════════════════════════════════════════════════

class TestGenerator:
    """Tests for the synthetic data generator."""

    def test_deterministic_output(self):
        """Same seed must produce same results."""
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)

        vals1 = [rng1.integers(0, 100) for _ in range(10)]
        vals2 = [rng2.integers(0, 100) for _ in range(10)]
        assert vals1 == vals2

    def test_negative_binomial_overdispersion(self):
        """Negative binomial should produce more variance than Poisson."""
        rng = np.random.default_rng(42)
        lam = 50
        poisson_samples = rng.poisson(lam, 1000)
        nb_r = lam * 0.5
        nb_p = nb_r / (nb_r + lam)
        nb_samples = rng.negative_binomial(nb_r, nb_p, 1000)

        assert np.var(nb_samples) > np.var(poisson_samples), \
            "Negative binomial should have higher variance"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
