from __future__ import annotations

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("VerifyUsage").master("local[1]").getOrCreate()

    scores = spark.read.parquet(
        "/opt/spark/data/usage/inclusive_mobility/mobility_scores/20260530"
    )
    print(f"mobility_scores rows: {scores.count()}")
    scores.select(
        "name", "city", "accessibility_score", "weather_risk_score", "mobility_score"
    ).show(5, truncate=False)

    risky = spark.read.parquet(
        "/opt/spark/data/usage/inclusive_mobility/risky_areas/20260530"
    )
    print(f"risky_areas rows: {risky.count()}")

    priorities = spark.read.parquet(
        "/opt/spark/data/usage/inclusive_mobility/improvement_priorities/20260530"
    )
    print(f"improvement_priorities rows: {priorities.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
