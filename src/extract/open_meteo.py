# src/extract/open_meteo.py
from __future__ import annotations
import requests
from typing import Dict, Any, List

BASE_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_hourly_forecast(
    latitude: float,
    longitude: float,
    timezone: str = "UTC",
    hourly: List[str] | None = None,
) -> Dict[str, Any]:
    if hourly is None:
        hourly = ["temperature_2m", "precipitation", "windspeed_10m", "weathercode"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone,
        "hourly": ",".join(hourly),
    }

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()