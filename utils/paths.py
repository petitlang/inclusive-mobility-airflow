from __future__ import annotations

from datetime import date
from pathlib import Path

from utils.config import DATA_ROOT


def current_day() -> str:
    return date.today().strftime("%Y%m%d")


def data_dir(layer: str, group: str, table_name: str, day: str | None = None) -> Path:
    partition_day = day or current_day()
    return DATA_ROOT / layer / group / table_name / partition_day


def ensure_data_dir(
    layer: str,
    group: str,
    table_name: str,
    day: str | None = None,
) -> Path:
    target = data_dir(layer, group, table_name, day)
    target.mkdir(parents=True, exist_ok=True)
    return target
