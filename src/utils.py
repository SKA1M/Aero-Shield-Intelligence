"""
Aero Shield v1.8 — Utility Functions
=====================================
API fetching, data enrichment, and helper utilities.
All API keys must be loaded from environment variables via load_config().
"""

from __future__ import annotations

import os
import time
import logging
from typing import Any

import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Population density constants (people/km2)
# Source: World Population Review 2024 — Indian metro district estimates
# These are representative fixed values, not sampled randomly.
# ---------------------------------------------------------------------------
POP_DENSITY_BY_CLASS: dict[str, int] = {
    "Dense Residential/Commercial": 22000,
    "High-Rise Business District":  15000,
    "Open Space/Park":               3000,
    "Urban Mixed":                   11000,
}

ASPECT_RATIO_BY_CLASS: dict[str, float] = {
    "Dense Residential/Commercial": 2.5,
    "High-Rise Business District":  3.5,
    "Open Space/Park":              0.3,
    "Urban Mixed":                  1.2,
}

# Known high-density residential/commercial corridors in Delhi NCR
_DENSE_RESIDENTIAL_KEYWORDS = ("Anand Vihar", "Punjabi Bagh", "R K Puram", "Rohini")
_HIGH_RISE_KEYWORDS = ("Gurugram", "Gurgaon", "Noida Sector")
_OPEN_SPACE_KEYWORDS = ("Zoo", "Park", "Garden", "Forest")


# ---------------------------------------------------------------------------
# Config / Auth
# ---------------------------------------------------------------------------

def load_config() -> dict[str, str]:
    """
    Load API keys from .env file or environment variables.

    Returns:
        Dict with keys: OPENAQ_API_KEY, WEATHER_API_KEY, TOMTOM_API_KEY.

    Raises:
        EnvironmentError: If any required key is missing.
    """
    load_dotenv()

    required_keys = ("OPENAQ_API_KEY", "WEATHER_API_KEY", "TOMTOM_API_KEY")
    config = {}
    missing = []

    for key in required_keys:
        value = os.environ.get(key)
        if not value:
            missing.append(key)
        else:
            config[key] = value

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example to .env and fill in your API keys."
        )

    return config


# ---------------------------------------------------------------------------
# OpenAQ v3 — Pollution Layer
# ---------------------------------------------------------------------------

def fetch_pollution_data(
    api_key: str,
    country_id: int = 9,   # India
    limit: int = 25,
    target_year_month: str = "2026-03",
) -> pd.DataFrame:
    """
    Harvest live multi-pollutant profiles from OpenAQ v3 API.

    Only includes locations with verified readings from target_year_month.
    Only includes locations that have a valid PM2.5 reading.

    Args:
        api_key:           OpenAQ v3 API key.
        country_id:        OpenAQ country ID (9 = India).
        limit:             Max locations to query.
        target_year_month: YYYY-MM string to filter for recent data.

    Returns:
        DataFrame with columns:
            location_name, latitude, longitude,
            no2, o3, pm10, pm25, last_updated_local
    """
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    url_locations = "https://api.openaq.org/v3/locations"

    response = requests.get(
        url_locations,
        headers=headers,
        params={"countries_id": country_id, "limit": limit, "monitor": True},
        timeout=15,
    )
    response.raise_for_status()

    locations = response.json().get("results", [])
    logger.info(f"Found {len(locations)} candidate India sites.")

    records = []
    for loc in locations:
        loc_id = loc["id"]
        loc_name = loc["name"]
        lat = loc["coordinates"]["latitude"]
        lon = loc["coordinates"]["longitude"]

        url_latest = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
        res_latest = requests.get(url_latest, headers=headers, timeout=10)

        if res_latest.status_code != 200:
            continue

        latest_results = res_latest.json().get("results", [])
        record: dict[str, Any] = {
            "location_name": loc_name,
            "latitude": lat,
            "longitude": lon,
            "no2": None,
            "o3": None,
            "pm10": None,
            "pm25": None,
            "last_updated_local": None,
        }
        valid = False

        for r in latest_results:
            ts_local = r.get("datetime", {}).get("local")
            val = r.get("value")
            if ts_local and target_year_month in ts_local and val is not None and 0 <= val < 2000:
                valid = True
                record["last_updated_local"] = ts_local
                param_name = next(
                    (
                        s["parameter"]["name"]
                        for s in loc.get("sensors", [])
                        if s["id"] == r["sensorsId"]
                    ),
                    None,
                )
                if param_name in ("no2", "o3", "pm10", "pm25"):
                    record[param_name] = val

        if valid and record["pm25"] is not None:
            records.append(record)
            logger.info(f"  Mapped: {loc_name} (PM2.5: {record['pm25']})")

        time.sleep(0.05)  # Respect rate limit

    df = pd.DataFrame(records).drop_duplicates(subset=["location_name"]).head(15)
    logger.info(f"Layer 1 complete: {len(df)} valid sites with 2026 PM2.5 data.")
    return df


# ---------------------------------------------------------------------------
# OpenWeather — Meteorological Layer
# ---------------------------------------------------------------------------

def fetch_weather_data(df_locations: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Fetch meteorological data and calculate atmospheric stability metrics.

    Args:
        df_locations: DataFrame from fetch_pollution_data().
        api_key:      OpenWeatherMap API key.

    Returns:
        DataFrame with columns:
            location_name, last_updated_local, wind_speed_m_s,
            wind_direction_deg, pressure_hpa, mixing_height_m,
            ventilation_index, temperature_c, humidity_percent
    """
    from src.models import calculate_mixing_height, calculate_ventilation_index

    records = []
    for _, row in df_locations.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        )
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            w = resp.json()

            wind_speed = w.get("wind", {}).get("speed", 1.5)
            temp = w.get("main", {}).get("temp", 28.0)
            pressure = w.get("main", {}).get("pressure", 1013.0)

            mixing_height = calculate_mixing_height(temp, pressure)
            ventilation_index = calculate_ventilation_index(wind_speed, mixing_height)

            records.append({
                "location_name": row["location_name"],
                "last_updated_local": row["last_updated_local"],
                "wind_speed_m_s": wind_speed,
                "wind_direction_deg": w.get("wind", {}).get("deg", 0),
                "pressure_hpa": pressure,
                "mixing_height_m": mixing_height,
                "ventilation_index": ventilation_index,
                "temperature_c": temp,
                "humidity_percent": w.get("main", {}).get("humidity", 45),
            })
        except Exception as exc:
            logger.warning(f"Weather fetch failed for {row['location_name']}: {exc}")

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# TomTom Pro — Urban Mobility Layer
# ---------------------------------------------------------------------------

def fetch_traffic_data(df_locations: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Fetch traffic flow data and Functional Road Class from TomTom Pro.

    Args:
        df_locations: DataFrame from fetch_pollution_data().
        api_key:      TomTom Pro API key.

    Returns:
        DataFrame with columns:
            location_name, last_updated_local, traffic_index,
            road_delay_sec, road_type, current_speed_kmh
    """
    records = []
    for _, row in df_locations.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        url = (
            f"https://api.tomtom.com/traffic/services/4/flowSegmentData"
            f"/absolute/12/json?key={api_key}&point={lat},{lon}"
        )
        resp = requests.get(url, timeout=10)

        if resp.status_code == 200:
            data = resp.json().get("flowSegmentData", {})
            cur_spd = data.get("currentSpeed", 0)
            ff_spd = data.get("freeFlowSpeed", 0)
            congestion = (
                round(((ff_spd - cur_spd) / ff_spd * 100), 1) if ff_spd > 0 else 0.0
            )
            records.append({
                "location_name": row["location_name"],
                "last_updated_local": row["last_updated_local"],
                "traffic_index": max(0, congestion),
                "road_delay_sec": data.get("currentDelay", 0),
                "road_type": data.get("frc", "FRC3"),
                "current_speed_kmh": cur_spd,
            })
        else:
            records.append({
                "location_name": row["location_name"],
                "last_updated_local": row["last_updated_local"],
                "traffic_index": 0,
                "road_delay_sec": 0,
                "road_type": "FRC3",
                "current_speed_kmh": 0,
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Urban Morphology Layer
# ---------------------------------------------------------------------------

def classify_urban_type(location_name: str) -> str:
    """
    Classify a location's urban type based on known area characteristics.

    Note: This is a rule-based classifier using curated keywords for
    Indian metro areas. Future work: replace with OpenStreetMap
    building footprint data for data-driven aspect ratios.

    Args:
        location_name: Name of the monitoring station / area.

    Returns:
        Urban class label string.
    """
    for keyword in _DENSE_RESIDENTIAL_KEYWORDS:
        if keyword.lower() in location_name.lower():
            return "Dense Residential/Commercial"
    for keyword in _HIGH_RISE_KEYWORDS:
        if keyword.lower() in location_name.lower():
            return "High-Rise Business District"
    for keyword in _OPEN_SPACE_KEYWORDS:
        if keyword.lower() in location_name.lower():
            return "Open Space/Park"
    return "Urban Mixed"


def get_aspect_ratio(urban_class: str) -> float:
    """Return representative Street Aspect Ratio for urban class."""
    return ASPECT_RATIO_BY_CLASS.get(urban_class, 1.2)


def get_population_density(urban_class: str) -> int:
    """
    Return representative population density (people/km2) for urban class.

    Uses fixed values sourced from World Population Review 2024 Indian
    metro district estimates — NOT randomly generated.
    """
    return POP_DENSITY_BY_CLASS.get(urban_class, 11000)


def build_morphology_layer(
    df_locations: pd.DataFrame,
    elevation_api_enabled: bool = True,
) -> pd.DataFrame:
    """
    Build urban morphology DataFrame from location data.

    Args:
        df_locations:          DataFrame from fetch_pollution_data().
        elevation_api_enabled: Whether to call OpenTopoData API for elevation.

    Returns:
        DataFrame with morphology columns including urban_class,
        street_aspect_ratio, pop_density_km2, elevation_meters.
    """
    records = []
    for _, row in df_locations.iterrows():
        urban_class = classify_urban_type(row["location_name"])
        elevation = 0.0

        if elevation_api_enabled:
            try:
                topo_url = (
                    f"https://api.opentopodata.org/v1/aster30m"
                    f"?locations={row['latitude']},{row['longitude']}"
                )
                res = requests.get(topo_url, timeout=5)
                if res.status_code == 200:
                    elevation = res.json().get("results", [{}])[0].get("elevation", 0.0)
            except Exception:
                pass  # Elevation is supplementary; proceed without it

        records.append({
            "location_name": row["location_name"],
            "last_updated_local": row["last_updated_local"],
            "elevation_meters": round(elevation, 1),
            "urban_class": urban_class,
            "street_aspect_ratio": get_aspect_ratio(urban_class),
            "pop_density_km2": get_population_density(urban_class),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Map Helpers
# ---------------------------------------------------------------------------

def build_popup_html(row: pd.Series) -> str:
    """
    Build Folium popup HTML for a location marker.

    Args:
        row: A row from the master DataFrame (must have toxic_status,
             pm25, shift_cigarette_equiv, cigs_prevented_per_shift,
             risk_tier, location_name).

    Returns:
        HTML string for folium.Popup.
    """
    status = row["toxic_status"]
    status_color = "#c0392b" if status == "TOXIC" else "#27ae60"
    tier_color = (
        "#c0392b" if "Critical" in row["risk_tier"] or "Extreme" in row["risk_tier"]
        else "#e67e22" if "High" in row["risk_tier"]
        else "#27ae60"
    )

    return f"""
    <div style="width:210px; font-family:Arial; font-size:13px;">
        <h4 style="margin:0 0 6px 0; font-size:14px;">{row['location_name']}</h4>
        <hr style="margin:4px 0; border-color:#ddd;">
        <b>Status:</b>
        <span style="color:{status_color}; font-weight:bold;">{status}</span><br>
        <b>PM2.5:</b> {row['pm25']} ug/m3<br>
        <b>Risk Tier:</b>
        <span style="color:{tier_color};">{row['risk_tier']}</span><br>
        <b>Shift Risk:</b> {row['shift_cigarette_equiv']} cigarette-equiv<br>
        <div style="background:#f5f5f5; padding:6px; margin-top:6px; border-radius:4px;">
            <b>Aero Shield Impact:</b><br>
            Prevents {row['cigs_prevented_per_shift']} cigs/shift
        </div>
    </div>
    """


def get_marker_color(toxic_status: str) -> str:
    """Return Folium marker color based on toxic status."""
    return "red" if toxic_status == "TOXIC" else "green"
