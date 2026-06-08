from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    """Parse verification options."""
    parser = argparse.ArgumentParser(description="Verify usage parquet outputs.")
    parser.add_argument(
        "--day",
        default=date.today().strftime("%Y%m%d"),
        help="Partition day in YYYYMMDD format. Defaults to local current day.",
    )
    parser.add_argument(
        "--usage-root",
        default="s3a://usage-data-mobility",
        help="Root for usage outputs. Defaults to the LocalStack S3 usage bucket.",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Create a Spark session that can read LocalStack S3A paths."""
    return (
        SparkSession.builder.appName("VerifyUsage")
        .master("local[1]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "dummy")
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    usage_root = args.usage_root.rstrip("/")
    day = args.day

    spark = build_spark()

    scores_path = f"{usage_root}/inclusive_mobility/mobility_scores/{day}"
    risky_path = f"{usage_root}/inclusive_mobility/risky_areas/{day}"
    priorities_path = f"{usage_root}/inclusive_mobility/improvement_priorities/{day}"
    city_summary_path = f"{usage_root}/inclusive_mobility/city_daily_summary/{day}"

    print(f"Verifying usage outputs for day={day}")

    scores = spark.read.parquet(scores_path)
    print(f"mobility_scores rows: {scores.count()}")
    scores.select(
        "name", "city", "accessibility_score", "weather_risk_score", "mobility_score"
    ).show(5, truncate=False)

    risky = spark.read.parquet(risky_path)
    print(f"risky_areas rows: {risky.count()}")

    priorities = spark.read.parquet(priorities_path)
    print(f"improvement_priorities rows: {priorities.count()}")

    city_summary = spark.read.parquet(city_summary_path)
    print(f"city_daily_summary rows: {city_summary.count()}")
    city_summary.select(
        "city",
        "weather_date",
        "avg_mobility_score",
        "risky_places_count",
        "recommendation",
        "main_risk_reason",
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
