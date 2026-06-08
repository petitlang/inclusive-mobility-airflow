from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, col, explode, lit, to_date, to_timestamp


def _build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "dummy")
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def format_daily_weather(raw_path: str, output_path: str) -> None:
    """Normalize raw Open-Meteo daily JSON from S3 into a parquet weather table.

    Args:
        raw_path: S3A path to daily_weather.json.
        output_path: S3A directory where Spark writes the formatted parquet.
    """
    spark = _build_spark("FormatOpenMeteoDailyWeather")

    raw_df = spark.read.option("multiLine", "true").json(raw_path)
    locations_df = raw_df.select(
        col("timezone"),
        to_timestamp(col("fetched_at")).alias("source_fetched_at_utc"),
        explode(col("locations")).alias("location"),
    )
    zipped = arrays_zip(
        col("location.response.daily.time"),
        col("location.response.daily.temperature_2m_max"),
        col("location.response.daily.temperature_2m_min"),
        col("location.response.daily.temperature_2m_mean"),
        col("location.response.daily.apparent_temperature_max"),
        col("location.response.daily.precipitation_sum"),
        col("location.response.daily.precipitation_hours"),
        col("location.response.daily.wind_speed_10m_max"),
        col("location.response.daily.wind_gusts_10m_max"),
        col("location.response.daily.weather_code"),
    )

    formatted = locations_df.select(
        col("location.city").alias("city"),
        col("location.latitude").cast("double").alias("latitude"),
        col("location.longitude").cast("double").alias("longitude"),
        col("timezone"),
        col("source_fetched_at_utc"),
        explode(zipped).alias("daily"),
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

    formatted.write.mode("overwrite").parquet(output_path)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Format Open-Meteo daily weather to parquet on S3.")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    format_daily_weather(args.raw_path, args.output_path)


if __name__ == "__main__":
    main()
