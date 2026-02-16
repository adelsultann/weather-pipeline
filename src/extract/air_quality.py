# src/extract/air_quality.py
from __future__ import annotations
import requests
from typing import Dict, Any, List, Optional

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

DEFAULT_HOURLY: List[str] = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "european_aqi",
    "us_aqi",
]

def fetch_air_quality(
    latitude: float,
    longitude: float,
    timezone: str = "UTC",
    hourly: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Fetch air quality data from Open-Meteo Air Quality API. Returns full JSON payload.
    """
    if hourly is None:
        hourly = DEFAULT_HOURLY

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone,
        "hourly": ",".join(hourly),
    }

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()
