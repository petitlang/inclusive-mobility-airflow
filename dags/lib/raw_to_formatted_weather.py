from __future__ import annotations

from lib.paths import ensure_datalake_dir


def format_weather_data(**kwargs) -> str:
    """Prepare the formatted Open-Meteo folder for parquet output."""
    target_dir = ensure_datalake_dir("formatted", "open_meteo", "daily_weather")
    target_file = target_dir / "weather.snappy.parquet"
    print(f"Formatted weather output will be written to: {target_file}")
    return str(target_file)
