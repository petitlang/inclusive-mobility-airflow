from __future__ import annotations

from utils.docker_spark import spark_submit_or_raise
from utils.paths import current_day
from utils.s3_utils import FORMATTED_BUCKET, USAGE_BUCKET


_SPARK_APP = "/opt/spark/transform/combine_mobility_data.py"


def calculate_mobility_score(
    accessibility_score: float,
    weather_risk_score: float,
) -> float:
    """Combine accessibility quality and weather risk into a 0-100 score."""
    score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
    return round(max(0, min(100, score)), 2)


def compute_daily_mobility_scores(**kwargs) -> str:
    """Run the Spark combination + scoring job reading from / writing to S3."""
    day = current_day()
    accessibility_path = f"s3a://{FORMATTED_BUCKET}/acces_libre/establishments/{day}"
    weather_path = f"s3a://{FORMATTED_BUCKET}/open_meteo/daily_weather/{day}"
    scores_output = f"s3a://{USAGE_BUCKET}/inclusive_mobility/mobility_scores/{day}"
    risky_output = f"s3a://{USAGE_BUCKET}/inclusive_mobility/risky_areas/{day}"
    priorities_output = f"s3a://{USAGE_BUCKET}/inclusive_mobility/improvement_priorities/{day}"

    print("Submitting Spark combination + scoring job (S3)...")

    spark_submit_or_raise(
        _SPARK_APP,
        {
            "--accessibility-path": accessibility_path,
            "--weather-path": weather_path,
            "--scores-output": scores_output,
            "--risky-output": risky_output,
            "--priorities-output": priorities_output,
        },
    )

    print(f"Mobility scoring complete -> {scores_output}")
    return scores_output
