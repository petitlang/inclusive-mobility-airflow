import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from utils.paths import data_dir


def test_accessibility_formatted_output_path_follows_convention():
    path = data_dir("formatted", "acces_libre", "establishments", "20260505")

    assert str(path).replace("\\", "/").endswith(
        "/formatted/acces_libre/establishments/20260505"
    )


def test_weather_formatted_output_path_follows_convention():
    path = data_dir("formatted", "open_meteo", "daily_weather", "20260505")

    assert str(path).replace("\\", "/").endswith(
        "/formatted/open_meteo/daily_weather/20260505"
    )
