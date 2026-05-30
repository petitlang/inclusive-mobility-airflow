from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_timestamp


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


def format_accessibility(raw_path: str, output_path: str) -> None:
    """Normalize raw AccesLibre JSON from S3 into a parquet establishments table.

    Args:
        raw_path: S3A path to establishments.json (e.g. s3a://raw-data-mobility/...).
        output_path: S3A directory where Spark writes the formatted parquet.
    """
    spark = _build_spark("FormatAccesLibre")

    raw_df = spark.read.option("multiLine", "true").json(raw_path)
    records = raw_df.select(
        col("fetched_at"),
        explode(col("pages.response.data")).alias("records"),
    ).select(col("fetched_at"), explode(col("records")).alias("record"))

    formatted = records.select(
        col("record.id").alias("establishment_id"),
        col("record.name").alias("name"),
        col("record.activite").alias("activity"),
        col("record.commune").alias("city"),
        col("record.postal_code").alias("postal_code"),
        col("record.code_insee").alias("insee_code"),
        col("record.latitude").cast("double").alias("latitude"),
        col("record.longitude").cast("double").alias("longitude"),
        col("record.entree_plain_pied").cast("boolean").alias("entrance_flat_access"),
        col("record.entree_largeur_mini").cast("double").alias("entrance_min_width_cm"),
        col("record.entree_pmr").cast("boolean").alias("entrance_wheelchair_accessible"),
        col("record.stationnement_ext_pmr").cast("boolean").alias("external_disabled_parking"),
        col("record.sanitaires_presence").cast("boolean").alias("toilets_available"),
        col("record.sanitaires_adaptes").cast("boolean").alias("accessible_toilets"),
        col("record.web_url").alias("web_url"),
        to_timestamp(col("fetched_at")).alias("source_fetched_at_utc"),
        lit("acces_libre").alias("source"),
    )

    formatted.write.mode("overwrite").parquet(output_path)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Format AccesLibre raw JSON to parquet on S3.")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()
    format_accessibility(args.raw_path, args.output_path)


if __name__ == "__main__":
    main()
