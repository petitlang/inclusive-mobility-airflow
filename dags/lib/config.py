from __future__ import annotations

import os
from pathlib import Path


DATALAKE_ROOT = Path(os.getenv("DATALAKE_ROOT", "/opt/airflow/datalake"))

ACCES_LIBRE_API_URL = "https://acceslibre.beta.gouv.fr/api/erps/"
OPEN_METEO_API_URL = "https://api.open-meteo.com/v1/forecast"

DEFAULT_CITY = "Paris"
DEFAULT_LATITUDE = 48.8566
DEFAULT_LONGITUDE = 2.3522
