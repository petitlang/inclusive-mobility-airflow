from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from utils.config import (
    DEFAULT_CITY,
    DEFAULT_LATITUDE,
    DEFAULT_LONGITUDE,
    OPEN_METEO_DAILY_VARIABLES,
    OPEN_METEO_FORECAST_DAYS,
    OPEN_METEO_MAX_LOCATIONS,
    OPEN_METEO_API_URL,
    OPEN_METEO_TIMEZONE,
)
from utils.paths import data_dir, ensure_data_dir
from utils.s3_utils import upload_json, s3_key, layer_bucket


def build_open_meteo_url(
    latitude: float = DEFAULT_LATITUDE,
    longitude: float = DEFAULT_LONGITUDE,
    forecast_days: int = OPEN_METEO_FORECAST_DAYS,
    timezone_name: str = OPEN_METEO_TIMEZONE,
) -> str:
    """Build the Open-Meteo daily forecast URL.

    Args:
        latitude: Forecast latitude.
        longitude: Forecast longitude.
        forecast_days: Number of forecast days to request.
        timezone_name: IANA timezone used by Open-Meteo in the response.

    Returns:
        Fully qualified HTTPS URL for daily forecast data.
    """
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
    """Request JSON from an HTTP endpoint.

    Args:
        url: HTTP or HTTPS URL to call.
        timeout: Network timeout in seconds.

    Returns:
        Decoded JSON object as a dictionary.
    """
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def _valid_coordinate(latitude: Any, longitude: Any) -> bool:
    try:
        lat = float(latitude)
        lon = float(longitude)
    except (TypeError, ValueError):
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180


def _load_accessibility_records() -> list[dict[str, Any]]:
    raw_file = data_dir("raw", "acces_libre", "establishments") / "establishments.json"
    if not raw_file.exists():
        print(f"Accessibility raw file not found, using default city only: {raw_file}")
        return []

    payload = json.loads(raw_file.read_text(encoding="utf-8"))
    return [
        record
        for page in payload.get("pages", [])
        for record in page.get("response", {}).get("data", [])
    ]


def build_weather_locations(
    records: list[dict[str, Any]],
    max_locations: int = OPEN_METEO_MAX_LOCATIONS,
) -> list[dict[str, Any]]:
    """Build unique weather lookup locations from AccesLibre records.

    Locations are deduplicated by city name and use the first valid
    latitude/longitude observed for that city.
    """
    locations: dict[str, dict[str, Any]] = {}
    for record in records:
        city = (record.get("commune") or "").strip()
        latitude = record.get("latitude")
        longitude = record.get("longitude")
        if not city or not _valid_coordinate(latitude, longitude):
            continue

        city_key = city.casefold()
        if city_key not in locations:
            locations[city_key] = {
                "city": city,
                "latitude": float(latitude),
                "longitude": float(longitude),
            }

        if len(locations) >= max_locations:
            break

    if DEFAULT_CITY.casefold() not in locations:
        locations[DEFAULT_CITY.casefold()] = {
            "city": DEFAULT_CITY,
            "latitude": DEFAULT_LATITUDE,
            "longitude": DEFAULT_LONGITUDE,
        }

    return list(locations.values())


def fetch_weather_data(**kwargs) -> str:
    """Fetch Open-Meteo daily weather for AccesLibre cities into the Data Lake.

    Args:
        **kwargs: Airflow context arguments, currently unused.

    Returns:
        String path to the raw JSON file written.

    Side effects:
        Creates `data/raw/open_meteo/daily_weather/YYYYMMDD/daily_weather.json`.
    """
    target_dir = ensure_data_dir("raw", "open_meteo", "daily_weather")
    target_file = target_dir / "daily_weather.json"

    records = _load_accessibility_records()
    weather_locations = build_weather_locations(records)
    fetched_at = datetime.now(timezone.utc).isoformat()
    location_payloads = []
    for location in weather_locations:
        url = build_open_meteo_url(
            latitude=location["latitude"],
            longitude=location["longitude"],
        )
        response = request_json(url)
        location_payloads.append(
            {
                "city": location["city"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "request_url": url,
                "response": response,
            }
        )

    payload = {
        "source": "open_meteo",
        "api_url": OPEN_METEO_API_URL,
        "timezone": OPEN_METEO_TIMEZONE,
        "forecast_days": OPEN_METEO_FORECAST_DAYS,
        "max_locations": OPEN_METEO_MAX_LOCATIONS,
        "daily_variables": list(OPEN_METEO_DAILY_VARIABLES),
        "fetched_at": fetched_at,
        "location_count": len(location_payloads),
        "locations": location_payloads,
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        f"Wrote raw Open-Meteo weather for {len(location_payloads)} locations to: "
        f"{target_file}"
    )

    key = s3_key("raw", "open_meteo", "daily_weather", "daily_weather.json")
    upload_json(layer_bucket("raw"), key, payload)
    print(f"Uploaded to S3: s3://{layer_bucket('raw')}/{key}")

    return str(target_file)
