"""
Aero Shield — AWS Lambda Ingestion Handler
============================================
Harvests OpenAQ / OpenWeather / TomTom data and writes raw JSON to S3.

Deployment:
    Triggered by EventBridge schedule (hourly during peak pollution hours).
    Environment variables configured via AWS Lambda console or CDK.

S3 layout:
    s3://aero-shield-raw-zone/year=YYYY/month=MM/day=DD/hour=HH/
        pollution_<timestamp>.json
        weather_<timestamp>.json
        traffic_<timestamp>.json
"""

import json
import os
import logging
from datetime import datetime
from typing import Any

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

S3_RAW_BUCKET = os.environ["S3_RAW_BUCKET"]
OPENAQ_API_KEY = os.environ["OPENAQ_API_KEY"]
WEATHER_API_KEY = os.environ["WEATHER_API_KEY"]
TOMTOM_API_KEY = os.environ["TOMTOM_API_KEY"]


def _s3_key(layer: str, timestamp: datetime) -> str:
    """Build partitioned S3 key for Hive-style partition discovery."""
    return (
        f"year={timestamp.year:04d}/month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/hour={timestamp.hour:02d}/"
        f"{layer}_{timestamp.strftime('%Y%m%dT%H%M%S')}.json"
    )


def _write_s3(key: str, data: Any) -> None:
    """Write JSON-serialisable data to the raw S3 zone."""
    s3.put_object(
        Bucket=S3_RAW_BUCKET,
        Key=key,
        Body=json.dumps(data, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Wrote s3://{S3_RAW_BUCKET}/{key}")


def _fetch_pollution() -> list:
    """Fetch OpenAQ v3 locations and their latest readings."""
    headers = {"X-API-Key": OPENAQ_API_KEY, "Accept": "application/json"}
    resp = requests.get(
        "https://api.openaq.org/v3/locations",
        headers=headers,
        params={"countries_id": 9, "limit": 25, "monitor": True},
        timeout=15,
    )
    resp.raise_for_status()
    locations = resp.json().get("results", [])

    enriched = []
    for loc in locations:
        latest = requests.get(
            f"https://api.openaq.org/v3/locations/{loc['id']}/latest",
            headers=headers,
            timeout=10,
        )
        if latest.status_code == 200:
            loc["latest_readings"] = latest.json().get("results", [])
            enriched.append(loc)

    return enriched


def _fetch_weather(locations: list) -> list:
    """Fetch OpenWeather data for each location."""
    records = []
    for loc in locations:
        lat = loc["coordinates"]["latitude"]
        lon = loc["coordinates"]["longitude"]
        try:
            r = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"lat": lat, "lon": lon, "appid": WEATHER_API_KEY, "units": "metric"},
                timeout=10,
            )
            r.raise_for_status()
            records.append({
                "location_id": loc["id"],
                "location_name": loc["name"],
                "data": r.json(),
            })
        except Exception as exc:
            logger.warning(f"Weather fetch failed for {loc['name']}: {exc}")
    return records


def _fetch_traffic(locations: list) -> list:
    """Fetch TomTom Pro Traffic data for each location."""
    records = []
    for loc in locations:
        lat = loc["coordinates"]["latitude"]
        lon = loc["coordinates"]["longitude"]
        try:
            r = requests.get(
                "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/12/json",
                params={"key": TOMTOM_API_KEY, "point": f"{lat},{lon}"},
                timeout=10,
            )
            if r.status_code == 200:
                records.append({
                    "location_id": loc["id"],
                    "location_name": loc["name"],
                    "data": r.json().get("flowSegmentData", {}),
                })
        except Exception as exc:
            logger.warning(f"Traffic fetch failed for {loc['name']}: {exc}")
    return records


def lambda_handler(event: dict, context: Any) -> dict:
    """
    Lambda entry point. Fetches all three API layers and writes to S3.

    Args:
        event:   Lambda event (EventBridge, API Gateway, or manual invocation).
        context: Lambda runtime context.

    Returns:
        Dict with status and S3 keys written.
    """
    timestamp = datetime.utcnow()
    logger.info(f"Starting ingestion at {timestamp.isoformat()}")

    try:
        pollution = _fetch_pollution()
        pollution_key = _s3_key("pollution", timestamp)
        _write_s3(pollution_key, pollution)

        weather = _fetch_weather(pollution)
        weather_key = _s3_key("weather", timestamp)
        _write_s3(weather_key, weather)

        traffic = _fetch_traffic(pollution)
        traffic_key = _s3_key("traffic", timestamp)
        _write_s3(traffic_key, traffic)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "ingestion_timestamp": timestamp.isoformat(),
                "keys_written": {
                    "pollution": pollution_key,
                    "weather": weather_key,
                    "traffic": traffic_key,
                },
                "site_count": len(pollution),
            }),
        }
    except Exception as exc:
        logger.error(f"Ingestion failed: {exc}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "error": str(exc)}),
        }
