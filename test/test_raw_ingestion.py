import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.acces_libre_fetcher import build_acces_libre_url
from ingestion.open_meteo_fetcher import build_open_meteo_url


def test_build_acces_libre_url_uses_small_paginated_resource_request():
    url = build_acces_libre_url(page=2, page_size=25)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert "tabular-api.data.gouv.fr" in parsed.netloc
    assert query["page"] == ["2"]
    assert query["page_size"] == ["25"]


def test_build_open_meteo_url_requests_daily_weather_variables():
    url = build_open_meteo_url(forecast_days=1)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert "api.open-meteo.com" in parsed.netloc
    assert query["forecast_days"] == ["1"]
    assert query["timezone"] == ["Europe/Paris"]
    assert "precipitation_sum" in query["daily"][0]
    assert "wind_speed_10m_max" in query["daily"][0]
