from __future__ import annotations

from utils.docker_spark import spark_submit_or_raise
from utils.paths import data_dir


_SPARK_APP = "/opt/spark/transform/format_accessibility.py"


def _spark_path(airflow_path: str) -> str:
    """Convert /opt/airflow/data -> /opt/spark/data for spark-submit args."""
    return airflow_path.replace("/opt/airflow/data", "/opt/spark/data")


def format_accessibility_data(**kwargs) -> str:
    """Run the AccesLibre Spark formatting job via docker-spark-submit.

    Returns:
        String path to the formatted parquet dataset directory.
    """
    raw_input = data_dir("raw", "acces_libre", "establishments") / "establishments.json"
    output_dir = data_dir("formatted", "acces_libre", "establishments")

    print(f"Submitting Spark formatting job for accessibility data...")
    print(f"  Airflow raw:  {raw_input}")
    print(f"  Airflow out:  {output_dir}")

    spark_submit_or_raise(
        _SPARK_APP,
        {
            "--raw-path": _spark_path(str(raw_input)),
            "--output-path": _spark_path(str(output_dir)),
        },
    )

    print(f"Accessibility formatting complete: {output_dir}")
    return str(output_dir)
