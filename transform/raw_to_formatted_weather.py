from __future__ import annotations

from utils.docker_spark import spark_submit_or_raise
from utils.paths import current_day
from utils.s3_utils import RAW_BUCKET, FORMATTED_BUCKET


_SPARK_APP = "/opt/spark/transform/format_weather.py"


def format_weather_data(**kwargs) -> str:
    """Run the Open-Meteo Spark formatting job reading from / writing to S3."""
    day = current_day()
    raw_path = f"s3a://{RAW_BUCKET}/open_meteo/daily_weather/{day}/daily_weather.json"
    output_path = f"s3a://{FORMATTED_BUCKET}/open_meteo/daily_weather/{day}"

    print(f"Submitting Spark formatting (S3) for weather data...")
    print(f"  S3 raw:     {raw_path}")
    print(f"  S3 output:  {output_path}")

    spark_submit_or_raise(
        _SPARK_APP,
        {"--raw-path": raw_path, "--output-path": output_path},
    )

    print(f"Weather formatting complete -> {output_path}")
    return output_path
