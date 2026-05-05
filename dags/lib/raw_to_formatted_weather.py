from __future__ import annotations

from lib.paths import datalake_dir


def format_weather_data(**kwargs) -> str:
    """Return the expected formatted Open-Meteo parquet output path.

    Args:
        **kwargs: Airflow context arguments, currently unused.

    Returns:
        String path to the formatted parquet dataset directory.

    Side effects:
        Logs the raw input and formatted output paths expected by the Spark
        formatting job. The parquet files are produced by
        `spark/jobs/format_weather.py`.
    """
    target_dir = datalake_dir("formatted", "open_meteo", "daily_weather")
    raw_input = datalake_dir("raw", "open_meteo", "daily_weather") / "weather.json"
    print(f"Spark input for weather formatting: {raw_input}")
    print(f"Formatted weather parquet output directory: {target_dir}")
    return str(target_dir)
