from __future__ import annotations

from lib.paths import datalake_dir


def format_accessibility_data(**kwargs) -> str:
    """Return the expected formatted AccesLibre parquet output path.

    Args:
        **kwargs: Airflow context arguments, currently unused.

    Returns:
        String path to the formatted parquet dataset directory.

    Side effects:
        Logs the raw input and formatted output paths expected by the Spark
        formatting job. The parquet files are produced by
        `spark/jobs/format_accessibility.py`.
    """
    target_dir = datalake_dir("formatted", "acces_libre", "establishments")
    raw_input = datalake_dir("raw", "acces_libre", "establishments") / "accessibility.json"
    print(f"Spark input for accessibility formatting: {raw_input}")
    print(f"Formatted accessibility parquet output directory: {target_dir}")
    return str(target_dir)
