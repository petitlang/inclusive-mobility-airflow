import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from transform.mobility_score import calculate_mobility_score


def test_calculate_mobility_score_balances_accessibility_and_weather_risk():
    assert calculate_mobility_score(80, 30) == 77.0


def test_calculate_mobility_score_stays_between_zero_and_one_hundred():
    assert calculate_mobility_score(200, -100) == 100
    assert calculate_mobility_score(-50, 200) == 0
