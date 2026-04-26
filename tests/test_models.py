"""
Aero Shield v1.8 — Unit Tests for Core Calculation Models
==========================================================
Run with: pytest tests/ -v
"""

import sys
import os
import pytest

# Ensure src is importable when running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.models import (
    calculate_mixing_height,
    calculate_ventilation_index,
    calculate_stagnation_penalty,
    calculate_source_penalty,
    calculate_inhaled_dose,
    calculate_cigarette_equivalence,
    calculate_protected_exposure,
    calculate_cigs_prevented,
    calculate_owpei,
    calculate_tam_index,
    classify_toxic_status,
    classify_risk_tier,
    STAGNATION_THRESHOLD,
    WHO_PM25_LIMIT,
    DEFAULT_FILTER_EFFICIENCY,
)


# ---------------------------------------------------------------------------
# Test: Cigarette Equivalence Formula
# ---------------------------------------------------------------------------

class TestCigaretteEquivalence:
    """
    Verify the Berkeley Earth cigarette equivalence calculation.

    Reference: 22 ug/m3 PM2.5 for 24 hrs = 1 cigarette.
    Active worker: 25 L/min = 1.5 m3/hr breathing rate.
    """

    def test_known_value_no_penalties(self):
        """
        Baseline check: 22 ug/m3, no penalties, 24-hr exposure = 1 cigarette.
        dose = 22 * 1.5 * 1.0 * 1.0 = 33 ug/hr
        cigs = (33 * 24) / (22 * 24) = 1.5
        (Note: 1.5x because 1.5 m3/hr vs 1.0 m3/hr assumed in Berkeley Earth)
        """
        dose = calculate_inhaled_dose(22.0, 1.0, 1.0)
        assert dose == pytest.approx(33.0, abs=0.01)
        cigs = calculate_cigarette_equivalence(dose, shift_hours=24)
        assert cigs == pytest.approx(1.5, abs=0.01)

    def test_gurugram_published_reading(self):
        """
        Reproduce the published 7.85 cigs result (Vikas Sadan, Gurugram).
        At ~177 ug/m3 PM2.5 with stagnant air and arterial road penalties.
        """
        pm25 = 177.0
        stag = calculate_stagnation_penalty(500.0)
        src = calculate_source_penalty("FRC1")
        dose = calculate_inhaled_dose(pm25, stag, src)
        cigs = calculate_cigarette_equivalence(dose)
        assert 7.0 <= cigs <= 9.0, f"Expected ~7.85 cigs, got {cigs}"

    def test_extreme_pollution_is_critical_risk(self):
        """300 ug/m3 with all penalties produces Critical Risk (>10 cigs)."""
        pm25 = 300.0
        stag = calculate_stagnation_penalty(500.0)
        src = calculate_source_penalty("FRC1")
        dose = calculate_inhaled_dose(pm25, stag, src)
        cigs = calculate_cigarette_equivalence(dose)
        assert cigs > 10.0
        assert classify_risk_tier(cigs) == "Critical Risk"

    def test_zero_pm25_gives_zero_cigs(self):
        """Zero pollution = zero cigarette equivalents."""
        dose = calculate_inhaled_dose(0.0, 1.0, 1.0)
        assert dose == 0.0
        cigs = calculate_cigarette_equivalence(0.0)
        assert cigs == 0.0

    def test_shift_hours_scales_linearly(self):
        """Doubling shift hours should double cigarette equivalents."""
        dose = calculate_inhaled_dose(50.0, 1.0, 1.0)
        cigs_8hr = calculate_cigarette_equivalence(dose, shift_hours=8)
        cigs_16hr = calculate_cigarette_equivalence(dose, shift_hours=16)
        assert cigs_16hr == pytest.approx(cigs_8hr * 2, rel=0.01)


# ---------------------------------------------------------------------------
# Test: OWPEI Calculation
# ---------------------------------------------------------------------------

class TestOWPEI:
    """Verify the Outdoor Worker Pollution Exposure Index formula."""

    def test_owpei_formula_manual(self):
        """
        Manual verification:
        PM2.5=100, stagnation=1.5, source=1.3, SAR=2.0
        OWPEI = 100 * 1.5 * 1.3 * (2.0/2) = 195.0
        """
        result = calculate_owpei(
            pm25_ug_m3=100.0,
            stagnation_penalty=1.5,
            source_penalty=1.3,
            street_aspect_ratio=2.0,
        )
        assert result == pytest.approx(195.0, abs=0.1)

    def test_owpei_scales_with_pm25(self):
        """Doubling PM2.5 should double OWPEI (all else equal)."""
        owpei_100 = calculate_owpei(100.0, 1.0, 1.1, 1.2)
        owpei_200 = calculate_owpei(200.0, 1.0, 1.1, 1.2)
        assert owpei_200 == pytest.approx(owpei_100 * 2, rel=0.01)

    def test_owpei_open_park_is_lower(self):
        """
        Open space (low SAR=0.3) should produce much lower OWPEI
        than a dense canyon (SAR=3.5) for the same pollution level.
        """
        owpei_park = calculate_owpei(80.0, 1.0, 1.1, 0.3)
        owpei_canyon = calculate_owpei(80.0, 1.5, 1.3, 3.5)
        assert owpei_park < owpei_canyon

    def test_owpei_nonnegative(self):
        """OWPEI should never be negative."""
        result = calculate_owpei(0.0, 1.0, 1.0, 0.0)
        assert result >= 0.0


# ---------------------------------------------------------------------------
# Test: Stagnation Penalty
# ---------------------------------------------------------------------------

class TestStagnationPenalty:
    """Verify the stagnation penalty threshold logic."""

    def test_stagnant_air_returns_1_5(self):
        """VI below threshold (1000) should return 1.5 penalty."""
        assert calculate_stagnation_penalty(999.9) == 1.5
        assert calculate_stagnation_penalty(0.0) == 1.5
        assert calculate_stagnation_penalty(500.0) == 1.5

    def test_good_ventilation_returns_1_0(self):
        """VI at or above threshold (1000) should return 1.0."""
        assert calculate_stagnation_penalty(1000.0) == 1.0
        assert calculate_stagnation_penalty(5000.0) == 1.0

    def test_threshold_boundary_exactly(self):
        """Exactly at threshold (1000.0) should NOT be stagnant."""
        assert calculate_stagnation_penalty(STAGNATION_THRESHOLD) == 1.0

    def test_penalty_is_one_of_two_values(self):
        """Penalty must always be exactly 1.0 or 1.5."""
        for vi in [0, 100, 999, 1000, 1001, 5000, 10000]:
            penalty = calculate_stagnation_penalty(float(vi))
            assert penalty in (1.0, 1.5), f"Unexpected penalty {penalty} for VI={vi}"


# ---------------------------------------------------------------------------
# Test: Source Penalty (Road Type)
# ---------------------------------------------------------------------------

class TestSourcePenalty:
    """Verify road-type source penalties."""

    def test_frc1_returns_1_3(self):
        assert calculate_source_penalty("FRC1") == 1.3

    def test_frc2_returns_1_3(self):
        assert calculate_source_penalty("FRC2") == 1.3

    def test_frc3_returns_1_1(self):
        assert calculate_source_penalty("FRC3") == 1.1

    def test_unknown_returns_1_1(self):
        assert calculate_source_penalty("FRC5") == 1.1
        assert calculate_source_penalty("N/A") == 1.1
        assert calculate_source_penalty("") == 1.1

    def test_case_sensitive(self):
        """Road type matching is case-sensitive (TomTom returns uppercase)."""
        assert calculate_source_penalty("frc1") == 1.1  # lowercase = not matched


# ---------------------------------------------------------------------------
# Test: Filter Efficiency & Protection
# ---------------------------------------------------------------------------

class TestFilterProtection:
    """Verify Aero Shield filter impact calculations."""

    def test_default_efficiency_reduces_by_32_5_percent(self):
        """At 32.5% efficiency, protected = 67.5% of baseline."""
        baseline = 10.0
        protected = calculate_protected_exposure(baseline)
        assert protected == pytest.approx(6.75, abs=0.01)

    def test_cigs_prevented_is_complement_of_protected(self):
        """prevented + protected should equal baseline."""
        baseline = 8.0
        prevented = calculate_cigs_prevented(baseline)
        protected = calculate_protected_exposure(baseline)
        assert prevented + protected == pytest.approx(baseline, abs=0.01)

    def test_100_percent_efficiency_means_zero_exposure(self):
        """Perfect filter: zero residual exposure."""
        assert calculate_protected_exposure(5.0, filter_efficiency=1.0) == 0.0

    def test_zero_efficiency_means_no_protection(self):
        """No filter: residual = baseline."""
        assert calculate_protected_exposure(5.0, filter_efficiency=0.0) == pytest.approx(5.0)

    def test_invalid_efficiency_raises_value_error(self):
        """Filter efficiency outside [0, 1] must raise ValueError."""
        with pytest.raises(ValueError):
            calculate_protected_exposure(5.0, filter_efficiency=1.5)
        with pytest.raises(ValueError):
            calculate_protected_exposure(5.0, filter_efficiency=-0.1)


# ---------------------------------------------------------------------------
# Test: TAM Index
# ---------------------------------------------------------------------------

class TestTAMIndex:
    """Verify TAM Index market scoring formula."""

    def test_tam_formula_manual(self):
        """TAM = (OWPEI x pop_density) / 1000. Manual check."""
        result = calculate_tam_index(owpei=200.0, pop_density_km2=15000)
        assert result == pytest.approx(3000.0, abs=0.1)

    def test_higher_density_means_higher_tam(self):
        """Same OWPEI, higher population density = higher launch priority."""
        tam_low = calculate_tam_index(100.0, 5000)
        tam_high = calculate_tam_index(100.0, 20000)
        assert tam_high > tam_low


# ---------------------------------------------------------------------------
# Test: Classification Helpers
# ---------------------------------------------------------------------------

class TestClassifiers:
    """Verify toxic status and risk tier classification."""

    def test_above_who_limit_is_toxic(self):
        assert classify_toxic_status(WHO_PM25_LIMIT + 0.1) == "TOXIC"

    def test_at_who_limit_is_safe(self):
        """Exactly at limit = not toxic (strictly greater than)."""
        assert classify_toxic_status(WHO_PM25_LIMIT) == "SAFE"

    def test_zero_pm25_is_safe(self):
        assert classify_toxic_status(0.0) == "SAFE"

    def test_risk_tiers_boundaries(self):
        assert classify_risk_tier(0.5) == "Low Risk"
        assert classify_risk_tier(1.0) == "Moderate Risk"
        assert classify_risk_tier(2.9) == "Moderate Risk"
        assert classify_risk_tier(3.0) == "High Risk"
        assert classify_risk_tier(5.9) == "High Risk"
        assert classify_risk_tier(6.0) == "Extreme Risk"
        assert classify_risk_tier(9.9) == "Extreme Risk"
        assert classify_risk_tier(10.0) == "Critical Risk"
        assert classify_risk_tier(50.0) == "Critical Risk"

    def test_toxic_status_is_binary(self):
        """classify_toxic_status must return exactly 'TOXIC' or 'SAFE'."""
        for pm25 in [0, 5, 15, 15.1, 50, 300]:
            result = classify_toxic_status(float(pm25))
            assert result in ("TOXIC", "SAFE"), f"Unexpected: {result} for PM2.5={pm25}"
