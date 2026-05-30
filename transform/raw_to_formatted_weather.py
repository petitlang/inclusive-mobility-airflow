from __future__ import annotations

from utils.docker_spark import spark_submit_or_raise
from utils.paths import data_dir


_SPARK_APP = "/opt/spark/transform/format_weather.py"


def _spark_path(airflow_path: str) -> str:
    """Convert /opt/airflow/data -> /opt/spark/data for spark-submit args."""
    return airflow_path.replace("/opt/airflow/data", "/opt/spark/data")


def format_weather_data(**kwargs) -> str:
    """Run the Open-Meteo Spark formatting job via docker-spark-submit.

    Returns:
        String path to the formatted parquet dataset directory.
    """
    raw_input = data_dir("raw", "open_meteo", "daily_weather") / "daily_weather.json"
    output_dir = data_dir("formatted", "open_meteo", "daily_weather")

    print(f"Submitting Spark formatting job for weather data...")
    print(f"  Airflow raw:  {raw_input}")
    print(f"  Airflow out:  {output_dir}")

    spark_submit_or_raise(
        _SPARK_APP,
        {
            "--raw-path": _spark_path(str(raw_input)),
            "--output-path": _spark_path(str(output_dir)),
        },
    )

    print(f"Weather formatting complete: {output_dir}")
    return str(output_dir)
