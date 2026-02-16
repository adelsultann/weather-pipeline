# src/extract/open_meteo.py
from __future__ import annotations

import requests
from typing import Dict, Any, List, Optional

BASE_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_HOURLY: List[str] = [
    "temperature_2m",
    "precipitation",
    "precipitation_probability",
    "wind_speed_10m",
    "weathercode",
    "relative_humidity_2m",
    "surface_pressure",
    "visibility",
    "uv_index",
]

DEFAULT_DAILY: List[str] = [
    "sunrise",
    "sunset",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "weathercode",
    "uv_index_max",
]

def fetch_hourly_forecast(
    latitude: float,
    longitude: float,
    timezone: str = "UTC",
    hourly: Optional[List[str]] = None,
    daily: Optional[List[str]] = None,
    forecast_days: int = 7,
) -> Dict[str, Any]:
    """
    Fetch forecast data from Open-Meteo. Returns full JSON payload.
    """
    if hourly is None:
        hourly = DEFAULT_HOURLY
    if daily is None:
        daily = DEFAULT_DAILY

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone,
        "hourly": ",".join(hourly),
        "daily": ",".join(daily),
        "forecast_days": forecast_days,
    }

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()