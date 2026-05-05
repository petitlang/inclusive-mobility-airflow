from __future__ import annotations

from lib.paths import ensure_datalake_dir


def format_accessibility_data(**kwargs) -> str:
    """Prepare the formatted AccesLibre folder for parquet output."""
    target_dir = ensure_datalake_dir("formatted", "acces_libre", "establishments")
    target_file = target_dir / "establishments.snappy.parquet"
    print(f"Formatted accessibility output will be written to: {target_file}")
    return str(target_file)
