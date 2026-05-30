from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def count_parquet_rows(accessibility_path: str, weather_path: str) -> tuple[int, int]:
    """Count rows in the Stage 5 formatted parquet outputs.

    Args:
        accessibility_path: Parquet dataset directory for formatted
            AccesLibre establishments.
        weather_path: Parquet dataset directory for formatted Open-Meteo daily
            weather rows.

    Returns:
        Tuple containing accessibility row count and weather row count.

    Side effects:
        Starts a local Spark session and reads the two parquet datasets.
    """
    spark = SparkSession.builder.appName("VerifyFormattedOutputs").getOrCreate()
    try:
        accessibility_count = spark.read.parquet(accessibility_path).count()
        weather_count = spark.read.parquet(weather_path).count()
        return accessibility_count, weather_count
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parse command-line options for the formatted output verifier.

    Args:
        None.

    Returns:
        Parsed arguments with accessibility and weather parquet paths.

    Side effects:
        Reads command-line arguments from the current process.
    """
    parser = argparse.ArgumentParser(description="Verify Stage 5 parquet outputs.")
    parser.add_argument("--accessibility-path", required=True)
    parser.add_argument("--weather-path", required=True)
    return parser.parse_args()


def main() -> None:
    """Run the formatted output verifier and print row counts.

    Args:
        None.

    Returns:
        None.

    Side effects:
        Prints row counts to stdout for review logs.
    """
    args = parse_args()
    accessibility_count, weather_count = count_parquet_rows(
        args.accessibility_path,
        args.weather_path,
    )
    print(f"accessibility_count={accessibility_count}")
    print(f"weather_count={weather_count}")


if __name__ == "__main__":
    main()
