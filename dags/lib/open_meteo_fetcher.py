from __future__ import annotations

import json

from lib.config import (
    DEFAULT_CITY,
    DEFAULT_LATITUDE,
    DEFAULT_LONGITUDE,
    OPEN_METEO_API_URL,
)
from lib.paths import ensure_datalake_dir


def fetch_weather_data(**kwargs) -> str:
    """Create the raw Open-Meteo landing folder."""
    target_dir = ensure_datalake_dir("raw", "open_meteo", "daily_weather")
    target_file = target_dir / "weather.json"
    payload = {
        "source": "open_meteo",
        "api_url": OPEN_METEO_API_URL,
        "city": DEFAULT_CITY,
        "latitude": DEFAULT_LATITUDE,
        "longitude": DEFAULT_LONGITUDE,
        "status": "placeholder",
        "next_step": "fetch daily precipitation, wind and temperature data",
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Prepared raw weather file: {target_file}")
    return str(target_file)
