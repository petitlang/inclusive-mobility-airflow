from __future__ import annotations

from utils.docker_spark import spark_submit_or_raise
from utils.paths import data_dir


_SPARK_APP = "/opt/spark/transform/combine_mobility_data.py"


def _spark_path(airflow_path: str) -> str:
    """Convert /opt/airflow/data -> /opt/spark/data for spark-submit args."""
    return airflow_path.replace("/opt/airflow/data", "/opt/spark/data")


def calculate_mobility_score(
    accessibility_score: float,
    weather_risk_score: float,
) -> float:
    """Combine accessibility quality and weather risk into a 0-100 score."""
    score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
    return round(max(0, min(100, score)), 2)


def compute_daily_mobility_scores(**kwargs) -> str:
    """Run the Spark combination + scoring job via docker-spark-submit.

    Produces three usage datasets:
      - mobility_scores: all establishments with computed scores
      - risky_areas: mobility_score < 40
      - improvement_priorities: accessibility_score < 50
    """
    accessibility_path = data_dir("formatted", "acces_libre", "establishments")
    weather_path = data_dir("formatted", "open_meteo", "daily_weather")
    scores_output = data_dir("usage", "inclusive_mobility", "mobility_scores")
    risky_output = data_dir("usage", "inclusive_mobility", "risky_areas")
    priorities_output = data_dir("usage", "inclusive_mobility", "improvement_priorities")

    print("Submitting Spark combination + scoring job...")

    spark_submit_or_raise(
        _SPARK_APP,
        {
            "--accessibility-path": _spark_path(str(accessibility_path)),
            "--weather-path": _spark_path(str(weather_path)),
            "--scores-output": _spark_path(str(scores_output)),
            "--risky-output": _spark_path(str(risky_output)),
            "--priorities-output": _spark_path(str(priorities_output)),
        },
    )

    print(f"Mobility scoring complete. Scores at: {scores_output}")
    return str(scores_output)
