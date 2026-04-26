"""
Aero Shield v1.8 — Great Expectations Validation Suite
========================================================
Defines data quality expectations for raw API data before
it is allowed to proceed to the Glue transformation layer.

Run standalone: python pipeline/validation/expectations_suite.py
Or invoked by the Airflow DAG validate_raw_data task.

Philosophy: Fail fast. Bad raw data must NEVER reach the curated zone
or Redshift — a health metric built on corrupt sensor data is worse
than no metric at all.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

import great_expectations as gx
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expectation Suites
# ---------------------------------------------------------------------------

def build_pollution_suite(context: gx.DataContext) -> None:
    """
    Expectations for raw pollution data from OpenAQ v3.

    Key checks:
    - Required columns must all be present.
    - PM2.5 must be a valid physical measurement (0–1000 µg/m3).
    - Location names must be non-null.
    - Coordinates must be within India's bounding box.
    """
    suite_name = "aero_shield.pollution_raw"

    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(
        batch_request=context.get_batch_request("pollution_raw"),
        expectation_suite_name=suite_name,
    )

    # Required columns
    for col in ["location_name", "latitude", "longitude", "pm25", "last_updated_local", "run_timestamp"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)

    # PM2.5 range: physically valid (0 to 1000 µg/m3)
    validator.expect_column_values_to_be_between(
        "pm25", min_value=0, max_value=1000,
        meta={"description": "PM2.5 must be a valid physical measurement."}
    )

    # Latitude within India (roughly 6°N to 38°N)
    validator.expect_column_values_to_be_between("latitude", min_value=6.0, max_value=38.0)

    # Longitude within India (roughly 68°E to 98°E)
    validator.expect_column_values_to_be_between("longitude", min_value=68.0, max_value=98.0)

    # No duplicate locations per run
    validator.expect_compound_columns_to_be_unique(["location_name", "run_timestamp"])

    # At least 5 records per run (guard against API outage)
    validator.expect_table_row_count_to_be_between(min_value=5, max_value=25)

    validator.save_expectation_suite()
    logger.info(f"Pollution expectation suite saved: {suite_name}")


def build_weather_suite(context: gx.DataContext) -> None:
    """
    Expectations for raw meteorological data from OpenWeatherMap.

    Key checks:
    - Wind speed must be physically plausible.
    - Temperature must be within Indian climate range.
    - Pressure must be in normal atmospheric range.
    """
    suite_name = "aero_shield.weather_raw"

    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(
        batch_request=context.get_batch_request("weather_raw"),
        expectation_suite_name=suite_name,
    )

    for col in ["location_name", "wind_speed_ms", "pressure_hpa", "temperature_c", "run_timestamp"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)

    # Wind speed: 0 to 40 m/s (above 40 = extreme typhoon, likely API error)
    validator.expect_column_values_to_be_between("wind_speed_ms", min_value=0, max_value=40)

    # Temperature: India range -20°C to 50°C
    validator.expect_column_values_to_be_between("temperature_c", min_value=-20, max_value=50)

    # Pressure: normal atmospheric range 900–1100 hPa
    validator.expect_column_values_to_be_between("pressure_hpa", min_value=900, max_value=1100)

    validator.save_expectation_suite()
    logger.info(f"Weather expectation suite saved: {suite_name}")


def build_traffic_suite(context: gx.DataContext) -> None:
    """
    Expectations for raw traffic data from TomTom Pro.

    Key checks:
    - Road type FRC must be a valid TomTom FRC string.
    - Speed values must be physically plausible.
    """
    suite_name = "aero_shield.traffic_raw"

    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name, overwrite_existing=True)

    validator = context.get_validator(
        batch_request=context.get_batch_request("traffic_raw"),
        expectation_suite_name=suite_name,
    )

    for col in ["location_name", "road_type_frc", "current_speed_kmh", "run_timestamp"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)

    # Road type must be a valid TomTom FRC
    validator.expect_column_values_to_be_in_set(
        "road_type_frc",
        value_set=["FRC0", "FRC1", "FRC2", "FRC3", "FRC4", "FRC5", "FRC6", "FRC3"],
    )

    # Speed: 0 to 200 km/h
    validator.expect_column_values_to_be_between("current_speed_kmh", min_value=0, max_value=200)
    validator.expect_column_values_to_be_between("free_flow_speed_kmh", min_value=0, max_value=200)

    validator.save_expectation_suite()
    logger.info(f"Traffic expectation suite saved: {suite_name}")


# ---------------------------------------------------------------------------
# Lightweight standalone validator (no GE context required)
# ---------------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, layer: str) -> dict[str, Any]:
    """
    Lightweight validation for use in the notebook or local testing.
    Does not require a full Great Expectations context.

    Args:
        df:    DataFrame to validate.
        layer: One of 'pollution', 'weather', 'traffic'.

    Returns:
        Dict with 'success' (bool) and 'failures' (list of str).
    """
    failures = []

    REQUIRED_COLS = {
        "pollution": ["location_name", "latitude", "longitude", "pm25"],
        "weather":   ["location_name", "wind_speed_ms", "pressure_hpa", "temperature_c"],
        "traffic":   ["location_name", "road_type_frc", "current_speed_kmh"],
    }

    RANGE_CHECKS = {
        "pollution": {"pm25": (0, 1000), "latitude": (6, 38), "longitude": (68, 98)},
        "weather":   {"wind_speed_ms": (0, 40), "temperature_c": (-20, 50), "pressure_hpa": (900, 1100)},
        "traffic":   {"current_speed_kmh": (0, 200)},
    }

    required = REQUIRED_COLS.get(layer, [])
    for col in required:
        if col not in df.columns:
            failures.append(f"Missing column: {col}")
        elif df[col].isnull().any():
            n_null = df[col].isnull().sum()
            failures.append(f"Column '{col}' has {n_null} null values.")

    for col, (lo, hi) in RANGE_CHECKS.get(layer, {}).items():
        if col in df.columns:
            oob = ((df[col] < lo) | (df[col] > hi)).sum()
            if oob > 0:
                failures.append(f"Column '{col}': {oob} values outside [{lo}, {hi}].")

    if layer == "pollution" and len(df) < 5:
        failures.append(f"Too few records: {len(df)} (minimum 5 required).")

    return {"success": len(failures) == 0, "failures": failures}


# ---------------------------------------------------------------------------
# Entry point for standalone run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Building Great Expectations suites for Aero Shield v1.8...")

    try:
        ctx = gx.get_context()
        build_pollution_suite(ctx)
        build_weather_suite(ctx)
        build_traffic_suite(ctx)
        logger.info("All expectation suites created successfully.")
    except Exception as exc:
        logger.warning(
            f"GE context not configured ({exc}). "
            "Use validate_dataframe() for local validation without a GE context."
        )
        logger.info("Lightweight validators are available via validate_dataframe().")
