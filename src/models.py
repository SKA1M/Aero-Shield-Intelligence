"""
Aero Shield v1.8 — Core Biomedical & Risk Calculation Models
=============================================================
All functions are pure (no I/O, no side effects) and fully unit-testable.

References:
- Cigarette equivalence: Berkeley Earth (2015) — ~22 µg/m3 PM2.5 approx 1 cigarette/day
- Active Worker Persona: 25 L/min breathing rate (3x resting adult)
- Filter efficiency: E11 Pleated Filter, 32.5% realistic reduction
  (rated efficiency minus Total Inward Leakage)
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHIFT_HOURS: float = 8.0
BREATHING_RATE_M3_PER_HR: float = 1.5    # 25 L/min active worker
BERKELEY_EARTH_FACTOR: float = 22.0       # ug/m3 per cigarette-equivalent
CIGS_PER_DAY_NORMALIZATION: float = 24.0  # Berkeley Earth uses daily average
DEFAULT_FILTER_EFFICIENCY: float = 0.325  # E11 filter mid-point (25-40%)
STAGNATION_THRESHOLD: float = 1000.0      # VI below this = stagnant air
WHO_PM25_LIMIT: float = 15.0              # WHO annual guideline ug/m3


# ---------------------------------------------------------------------------
# Atmospheric / Physics Models
# ---------------------------------------------------------------------------

def calculate_mixing_height(temp_c: float, pressure_hpa: float) -> float:
    """
    Estimate Planetary Boundary Layer (PBL) mixing height in metres.

    Warmer air creates thermal buoyancy that lifts the boundary layer;
    higher pressure suppresses it.

    Args:
        temp_c:       Ambient temperature in Celsius.
        pressure_hpa: Barometric pressure in hPa.

    Returns:
        Estimated mixing height in metres (minimum 10 m).
    """
    temp_factor = max(2.0, temp_c - 15.0)
    return round(max(10.0, (temp_factor * 150.0) / (pressure_hpa / 1000.0)), 1)


def calculate_ventilation_index(wind_speed_ms: float, mixing_height_m: float) -> float:
    """
    Ventilation Index - volume of air moving through the atmospheric canyon.

    VI < 1000: stagnant (pollutants accumulate)
    VI > 6000: well-ventilated

    Args:
        wind_speed_ms:   Wind speed in m/s.
        mixing_height_m: Estimated mixing height in metres.

    Returns:
        Ventilation Index (m2/s).
    """
    return round(wind_speed_ms * mixing_height_m, 1)


# ---------------------------------------------------------------------------
# Exposure Penalty Models
# ---------------------------------------------------------------------------

def calculate_stagnation_penalty(ventilation_index: float) -> float:
    """
    Penalty multiplier applied when air is stagnant (VI < threshold).

    Args:
        ventilation_index: From calculate_ventilation_index().

    Returns:
        1.5 if stagnant (VI < 1000), else 1.0.
    """
    return 1.5 if ventilation_index < STAGNATION_THRESHOLD else 1.0


def calculate_source_penalty(road_type: str) -> float:
    """
    Source proximity penalty based on TomTom Functional Road Class (FRC).

    Workers near arterial roads face elevated diesel-soot exposure.

    Args:
        road_type: TomTom FRC string (e.g. 'FRC1', 'FRC2', 'FRC3').

    Returns:
        1.3 for high-traffic arterials (FRC1/FRC2), else 1.1.
    """
    return 1.3 if road_type in ("FRC1", "FRC2") else 1.1


# ---------------------------------------------------------------------------
# Biomedical Impact Engine
# ---------------------------------------------------------------------------

def calculate_inhaled_dose(
    pm25_ug_m3: float,
    stagnation_penalty: float,
    source_penalty: float,
    breathing_rate_m3_hr: float = BREATHING_RATE_M3_PER_HR,
) -> float:
    """
    Total PM2.5 inhaled per hour by an active outdoor worker (ug/hr).

    Uses Active Worker Persona (25 L/min = 1.5 m3/hr) - 3x resting adult.

    Args:
        pm25_ug_m3:           Ambient PM2.5 concentration (ug/m3).
        stagnation_penalty:   From calculate_stagnation_penalty().
        source_penalty:       From calculate_source_penalty().
        breathing_rate_m3_hr: Override for non-standard worker personas.

    Returns:
        Inhaled PM2.5 dose in ug/hr.
    """
    return round(
        pm25_ug_m3 * breathing_rate_m3_hr * stagnation_penalty * source_penalty, 2
    )


def calculate_cigarette_equivalence(
    inhaled_dose_ug_hr: float,
    shift_hours: float = SHIFT_HOURS,
) -> float:
    """
    Convert inhaled PM2.5 dose into cigarette-equivalents per shift.

    Based on Berkeley Earth: breathing 22 ug/m3 PM2.5 for 24 hours = 1 cigarette.

    Formula: Cigs = (dose_ug/hr x shift_hrs) / (22 x 24)

    Args:
        inhaled_dose_ug_hr: Hourly inhaled PM2.5 dose (ug/hr).
        shift_hours:        Length of work shift in hours.

    Returns:
        Cigarette-equivalents for the full shift.
    """
    return round(
        (inhaled_dose_ug_hr * shift_hours)
        / (BERKELEY_EARTH_FACTOR * CIGS_PER_DAY_NORMALIZATION),
        2,
    )


def calculate_protected_exposure(
    baseline_cigs: float,
    filter_efficiency: float = DEFAULT_FILTER_EFFICIENCY,
) -> float:
    """
    Residual cigarette-equivalent exposure after Aero Shield filtration.

    Args:
        baseline_cigs:     Unprotected shift cigarette-equivalents.
        filter_efficiency: E11 filter realistic efficiency (default 32.5%).

    Returns:
        Protected shift cigarette-equivalents.

    Raises:
        ValueError: If filter_efficiency is outside [0, 1].
    """
    if not (0.0 <= filter_efficiency <= 1.0):
        raise ValueError(
            f"filter_efficiency must be in [0, 1]; got {filter_efficiency}"
        )
    return round(baseline_cigs * (1.0 - filter_efficiency), 2)


def calculate_cigs_prevented(
    baseline_cigs: float,
    filter_efficiency: float = DEFAULT_FILTER_EFFICIENCY,
) -> float:
    """
    Cigarettes-worth of inhalation prevented per shift by Aero Shield.

    Args:
        baseline_cigs:     Unprotected shift cigarette-equivalents.
        filter_efficiency: E11 filter realistic efficiency (default 32.5%).

    Returns:
        Cigarette-equivalents prevented per shift.
    """
    return round(baseline_cigs * filter_efficiency, 2)


# ---------------------------------------------------------------------------
# Composite Risk Indices
# ---------------------------------------------------------------------------

def calculate_owpei(
    pm25_ug_m3: float,
    stagnation_penalty: float,
    source_penalty: float,
    street_aspect_ratio: float,
) -> float:
    """
    Outdoor Worker Pollution Exposure Index (OWPEI).

    Composite risk score combining ambient pollution, atmospheric trapping,
    traffic-source proximity, and urban canyon geometry.

    Formula: OWPEI = PM2.5 x stagnation_penalty x source_penalty x (SAR / 2)

    Args:
        pm25_ug_m3:          Ambient PM2.5 concentration (ug/m3).
        stagnation_penalty:  From calculate_stagnation_penalty().
        source_penalty:      From calculate_source_penalty().
        street_aspect_ratio: Building height / street width ratio.

    Returns:
        OWPEI composite score.
    """
    return round(
        pm25_ug_m3 * stagnation_penalty * source_penalty * (street_aspect_ratio / 2.0),
        1,
    )


def calculate_tam_index(owpei: float, pop_density_km2: float) -> float:
    """
    Aero Shield TAM (Total Addressable Market) Index.

    Identifies high-priority launch markets by combining health risk
    intensity (OWPEI) with the density of at-risk workers.

    Formula: TAM_Index = (OWPEI x Population Density) / 1000

    Args:
        owpei:           OWPEI composite score.
        pop_density_km2: Population density (people/km2).

    Returns:
        TAM Index. Higher = higher launch priority.
    """
    return round((owpei * pop_density_km2) / 1000.0, 1)


# ---------------------------------------------------------------------------
# Classification Helpers
# ---------------------------------------------------------------------------

def classify_toxic_status(pm25_ug_m3: float) -> str:
    """
    Classify location as TOXIC or SAFE vs WHO PM2.5 guideline (15 ug/m3).

    Args:
        pm25_ug_m3: Ambient PM2.5 concentration.

    Returns:
        'TOXIC' or 'SAFE'.
    """
    return "TOXIC" if pm25_ug_m3 > WHO_PM25_LIMIT else "SAFE"


def classify_risk_tier(shift_cigs: float) -> str:
    """
    Human-readable risk tier based on shift cigarette-equivalents.

    Tiers: Low (<1) | Moderate (1-3) | High (3-6) | Extreme (6-10) | Critical (>10)

    Args:
        shift_cigs: From calculate_cigarette_equivalence().

    Returns:
        Risk tier label string.
    """
    if shift_cigs < 1.0:
        return "Low Risk"
    elif shift_cigs < 3.0:
        return "Moderate Risk"
    elif shift_cigs < 6.0:
        return "High Risk"
    elif shift_cigs < 10.0:
        return "Extreme Risk"
    return "Critical Risk"
