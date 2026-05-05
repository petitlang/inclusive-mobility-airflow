from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_timestamp


def format_accessibility(raw_path: str, output_path: str) -> None:
    """Normalize raw AccesLibre JSON into a parquet establishments table.

    Args:
        raw_path: Path to `accessibility.json` generated in the raw layer.
        output_path: Directory where Spark writes the formatted parquet dataset.

    Returns:
        None.

    Side effects:
        Overwrites the parquet dataset at `output_path`.
    """
    spark = SparkSession.builder.appName("FormatAccesLibre").getOrCreate()

    raw_df = spark.read.option("multiLine", "true").json(raw_path)
    records_df = raw_df.select(
        col("fetched_at"),
        explode(col("pages.response.data")).alias("records"),
    ).select(col("fetched_at"), explode(col("records")).alias("record"))

    formatted_df = records_df.select(
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

    formatted_df.write.mode("overwrite").parquet(output_path)
    spark.stop()


def main() -> None:
    """Run the AccesLibre Spark formatting job from command-line arguments.

    Args:
        None.

    Returns:
        None.

    Side effects:
        Reads CLI arguments and overwrites the requested formatted parquet
        output directory.
    """
    parser = argparse.ArgumentParser(description="Format AccesLibre raw JSON to parquet.")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    format_accessibility(args.raw_path, args.output_path)


if __name__ == "__main__":
    main()
