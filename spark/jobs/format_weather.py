from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, col, explode, lit, to_date, to_timestamp


def format_daily_weather(raw_path: str, output_path: str) -> None:
    """Normalize raw Open-Meteo daily JSON into a parquet weather table.

    Args:
        raw_path: Path to `weather.json` generated in the raw layer.
        output_path: Directory where Spark writes the formatted parquet dataset.

    Returns:
        None.

    Side effects:
        Overwrites the parquet dataset at `output_path`.
    """
    spark = SparkSession.builder.appName("FormatOpenMeteoDailyWeather").getOrCreate()

    raw_df = spark.read.option("multiLine", "true").json(raw_path)
    zipped_daily = arrays_zip(
        col("response.daily.time"),
        col("response.daily.temperature_2m_max"),
        col("response.daily.temperature_2m_min"),
        col("response.daily.temperature_2m_mean"),
        col("response.daily.apparent_temperature_max"),
        col("response.daily.precipitation_sum"),
        col("response.daily.precipitation_hours"),
        col("response.daily.wind_speed_10m_max"),
        col("response.daily.wind_gusts_10m_max"),
        col("response.daily.weather_code"),
    )

    formatted_df = raw_df.select(
        col("city"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("timezone"),
        to_timestamp(col("fetched_at")).alias("source_fetched_at_utc"),
        explode(zipped_daily).alias("daily"),
    ).select(
        to_date(col("daily.time")).alias("weather_date"),
        col("city"),
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("daily.temperature_2m_max").cast("double").alias("temperature_2m_max_c"),
        col("daily.temperature_2m_min").cast("double").alias("temperature_2m_min_c"),
        col("daily.temperature_2m_mean").cast("double").alias("temperature_2m_mean_c"),
        col("daily.apparent_temperature_max").cast("double").alias("apparent_temperature_max_c"),
        col("daily.precipitation_sum").cast("double").alias("precipitation_sum_mm"),
        col("daily.precipitation_hours").cast("double").alias("precipitation_hours"),
        col("daily.wind_speed_10m_max").cast("double").alias("wind_speed_10m_max_kmh"),
        col("daily.wind_gusts_10m_max").cast("double").alias("wind_gusts_10m_max_kmh"),
        col("daily.weather_code").cast("integer").alias("weather_code"),
        col("source_fetched_at_utc"),
        lit("open_meteo").alias("source"),
    )

    formatted_df.write.mode("overwrite").parquet(output_path)
    spark.stop()


def main() -> None:
    """Run the Open-Meteo daily weather Spark job from CLI arguments.

    Args:
        None.

    Returns:
        None.

    Side effects:
        Reads CLI arguments and overwrites the requested formatted parquet
        output directory.
    """
    parser = argparse.ArgumentParser(
        description="Format Open-Meteo daily weather raw JSON to parquet."
    )
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    format_daily_weather(args.raw_path, args.output_path)


if __name__ == "__main__":
    main()
