import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from kafka.consumers.weather_stream_to_raw import raw_stream_file
from kafka.producers.open_meteo_current_producer import build_open_meteo_current_url


def test_build_open_meteo_current_url_requests_current_weather_fields():
    url = build_open_meteo_current_url()
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert "api.open-meteo.com" in parsed.netloc
    assert query["timezone"] == ["Europe/Paris"]
    assert "temperature_2m" in query["current"][0]
    assert "wind_speed_10m" in query["current"][0]


def test_raw_stream_file_follows_data_convention():
    path = raw_stream_file("20260505")

    assert str(path).replace("\\", "/").endswith(
        "/data/raw/open_meteo/current_weather_stream/20260505/current_weather_stream.jsonl"
    )
