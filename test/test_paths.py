import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from lib.paths import datalake_dir


def test_datalake_path_convention():
    path = datalake_dir("raw", "acces_libre", "establishments", "20260505")

    assert str(path).replace("\\", "/").endswith(
        "/raw/acces_libre/establishments/20260505"
    )
