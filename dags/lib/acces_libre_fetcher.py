from __future__ import annotations

import json

from lib.config import ACCES_LIBRE_API_URL
from lib.paths import ensure_datalake_dir


def fetch_accessibility_data(**kwargs) -> str:
    """Create the raw AccesLibre landing folder.

    The next phase will replace this placeholder with an API call and write the
    raw response to accessibility.json.
    """
    target_dir = ensure_datalake_dir("raw", "acces_libre", "establishments")
    target_file = target_dir / "accessibility.json"
    payload = {
        "source": "acces_libre",
        "api_url": ACCES_LIBRE_API_URL,
        "status": "placeholder",
        "next_step": "fetch public establishment accessibility data",
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Prepared raw accessibility file: {target_file}")
    return str(target_file)
