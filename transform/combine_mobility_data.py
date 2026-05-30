from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    lit,
    round as spark_round,
    when,
)


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


def compute_accessibility_score(df: DataFrame) -> DataFrame:
    """Add accessibility_score (0-100) based on accessibility features.

    Scoring weights:
        entrance_wheelchair_accessible: 30 pts
        accessible_toilets:             25 pts
        external_disabled_parking:      20 pts
        entrance_flat_access:           15 pts
        entrance_min_width_cm:          10 pts (scaled, capped at 120cm)
    """
    width_score = spark_round(
        when(col("entrance_min_width_cm").isNull(), lit(0))
        .otherwise(
            when(col("entrance_min_width_cm") >= 120, lit(10))
            .otherwise(spark_round(col("entrance_min_width_cm") / 12, 1))
        ),
        1,
    )

    return df.withColumn(
        "accessibility_score",
        spark_round(
            when(col("entrance_wheelchair_accessible"), lit(30)).otherwise(lit(0))
            + when(col("accessible_toilets"), lit(25)).otherwise(lit(0))
            + when(col("external_disabled_parking"), lit(20)).otherwise(lit(0))
            + when(col("entrance_flat_access"), lit(15)).otherwise(lit(0))
            + width_score,
            1,
        ),
    )


def compute_weather_risk_score(df: DataFrame) -> DataFrame:
    """Add weather_risk_score (0-100, higher = more risk) based on weather conditions.

    Precipitation risk:   0-35 pts (10mm+ = max)
    Wind risk:            0-25 pts (50km/h+ = max)
    Temperature risk:     0-25 pts (<0°C or >35°C = max)
    Severe weather code:  0-15 pts (codes >= 70 = severe)
    """
    precip_score = spark_round(
        when(col("precipitation_sum_mm") >= 10, lit(35))
        .when(col("precipitation_sum_mm") >= 5, lit(20))
        .when(col("precipitation_sum_mm") >= 1, lit(10))
        .otherwise(lit(0)),
        1,
    )

    wind_score = spark_round(
        when(col("wind_speed_10m_max_kmh") >= 50, lit(25))
        .when(col("wind_speed_10m_max_kmh") >= 30, lit(15))
        .when(col("wind_speed_10m_max_kmh") >= 15, lit(5))
        .otherwise(lit(0)),
        1,
    )

    temp_score = spark_round(
        when(
            (col("temperature_2m_max_c") > 35) | (col("temperature_2m_min_c") < 0),
            lit(25),
        )
        .when(
            (col("temperature_2m_max_c") > 30) | (col("temperature_2m_min_c") < 5),
            lit(15),
        )
        .otherwise(lit(0)),
        1,
    )

    severe_code_score = spark_round(
        when(col("weather_code").isNotNull() & (col("weather_code") >= 70), lit(15))
        .when(col("weather_code").isNotNull() & (col("weather_code") >= 50), lit(8))
        .otherwise(lit(0)),
        1,
    )

    return df.withColumn(
        "weather_risk_score",
        spark_round(precip_score + wind_score + temp_score + severe_code_score, 1),
    )


def combine_and_score(
    accessibility_path: str,
    weather_path: str,
    scores_output: str,
    risky_output: str,
    priorities_output: str,
) -> None:
    """Join accessibility and weather data, compute scores, write 3 usage datasets.

    Args:
        accessibility_path: Formatted accessibility parquet directory.
        weather_path: Formatted daily weather parquet directory.
        scores_output: Output path for mobility_scores parquet.
        risky_output: Output path for risky_areas parquet.
        priorities_output: Output path for improvement_priorities parquet.
    """
    spark = _build_spark("CombineMobilityData")

    accessibility_df = spark.read.parquet(accessibility_path)
    weather_df = spark.read.parquet(weather_path)

    weather_join = weather_df.select(
        col("city").alias("weather_city"),
        col("weather_date"),
        col("temperature_2m_max_c"),
        col("temperature_2m_min_c"),
        col("precipitation_sum_mm"),
        col("wind_speed_10m_max_kmh"),
        col("weather_code"),
    )

    joined = accessibility_df.join(
        broadcast(weather_join),
        accessibility_df.city == weather_join.weather_city,
        "left",
    ).drop("weather_city")

    scored = compute_accessibility_score(joined)
    scored = compute_weather_risk_score(scored)

    scored = scored.withColumn(
        "mobility_score",
        spark_round(
            col("accessibility_score") * 0.7
            + (lit(100) - col("weather_risk_score")) * 0.3,
            1,
        ),
    )

    output_cols = [
        "establishment_id",
        "name",
        "activity",
        "city",
        "postal_code",
        "latitude",
        "longitude",
        "weather_date",
        "accessibility_score",
        "weather_risk_score",
        "mobility_score",
        "entrance_wheelchair_accessible",
        "accessible_toilets",
        "external_disabled_parking",
        "entrance_flat_access",
        "entrance_min_width_cm",
        "temperature_2m_max_c",
        "temperature_2m_min_c",
        "precipitation_sum_mm",
        "wind_speed_10m_max_kmh",
        "weather_code",
    ]

    result = scored.select(*[c for c in output_cols if c in scored.columns])

    result.write.mode("overwrite").parquet(scores_output)

    risky = result.filter(col("mobility_score") < 40)
    risky.write.mode("overwrite").parquet(risky_output)

    priorities = (
        result.filter(col("accessibility_score") < 50)
        .orderBy(col("accessibility_score").asc())
    )
    priorities.write.mode("overwrite").parquet(priorities_output)

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Combine mobility data and compute scores.")
    parser.add_argument("--accessibility-path", required=True)
    parser.add_argument("--weather-path", required=True)
    parser.add_argument("--scores-output", required=True)
    parser.add_argument("--risky-output", required=True)
    parser.add_argument("--priorities-output", required=True)
    args = parser.parse_args()

    combine_and_score(
        args.accessibility_path,
        args.weather_path,
        args.scores_output,
        args.risky_output,
        args.priorities_output,
    )


if __name__ == "__main__":
    main()
