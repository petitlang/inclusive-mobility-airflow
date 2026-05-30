from __future__ import annotations

import os
from pathlib import Path


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/opt/airflow/data"))

ACCES_LIBRE_API_URL = "https://acceslibre.beta.gouv.fr/api/erps/"
ACCES_LIBRE_TABULAR_API_URL = (
    "https://tabular-api.data.gouv.fr/api/resources/"
    "93ae96a7-1db7-4cb4-a9f1-6d778370b640/data/"
)
ACCES_LIBRE_PAGE_SIZE = int(os.getenv("ACCES_LIBRE_PAGE_SIZE", "100"))
ACCES_LIBRE_MAX_PAGES = int(os.getenv("ACCES_LIBRE_MAX_PAGES", "1"))

OPEN_METEO_API_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_max",
    "precipitation_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "weather_code",
)
OPEN_METEO_FORECAST_DAYS = int(os.getenv("OPEN_METEO_FORECAST_DAYS", "3"))
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Paris")
OPEN_METEO_CURRENT_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
)

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
WEATHER_STREAM_TOPIC = os.getenv("WEATHER_STREAM_TOPIC", "weather.raw.current")

DEFAULT_CITY = "Paris"
DEFAULT_LATITUDE = 48.8566
DEFAULT_LONGITUDE = 2.3522
