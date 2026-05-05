from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from lib.config import (
    DEFAULT_CITY,
    DEFAULT_LATITUDE,
    DEFAULT_LONGITUDE,
    OPEN_METEO_DAILY_VARIABLES,
    OPEN_METEO_FORECAST_DAYS,
    OPEN_METEO_API_URL,
    OPEN_METEO_TIMEZONE,
)
from lib.paths import ensure_datalake_dir


def build_open_meteo_url(
    latitude: float = DEFAULT_LATITUDE,
    longitude: float = DEFAULT_LONGITUDE,
    forecast_days: int = OPEN_METEO_FORECAST_DAYS,
    timezone_name: str = OPEN_METEO_TIMEZONE,
) -> str:
    query = urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "daily": ",".join(OPEN_METEO_DAILY_VARIABLES),
            "timezone": timezone_name,
            "forecast_days": forecast_days,
        }
    )
    return f"{OPEN_METEO_API_URL}?{query}"


def request_json(url: str, timeout: int = 30) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def fetch_weather_data(**kwargs) -> str:
    """Fetch raw daily weather data from Open-Meteo."""
    target_dir = ensure_datalake_dir("raw", "open_meteo", "daily_weather")
    target_file = target_dir / "weather.json"
    url = build_open_meteo_url()
    response = request_json(url)
    payload = {
        "source": "open_meteo",
        "api_url": OPEN_METEO_API_URL,
        "request_url": url,
        "city": DEFAULT_CITY,
        "latitude": DEFAULT_LATITUDE,
        "longitude": DEFAULT_LONGITUDE,
        "timezone": OPEN_METEO_TIMEZONE,
        "forecast_days": OPEN_METEO_FORECAST_DAYS,
        "daily_variables": list(OPEN_METEO_DAILY_VARIABLES),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "response": response,
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote raw Open-Meteo weather data to: {target_file}")
    return str(target_file)
