"""Create Kibana index patterns (data views) for the inclusive mobility indices.

Run once after Elasticsearch is populated. Can be called from Airflow DAG
or standalone via command line.
"""

from __future__ import annotations

import time
from urllib.request import Request, urlopen
from urllib.error import URLError
import json

KIBANA_URL = "http://kibana:5601"

INDEX_PATTERNS = [
    {
        "id": "inclusive_mobility_scores",
        "title": "inclusive_mobility_scores",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_risky_areas",
        "title": "inclusive_mobility_risky_areas",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_improvement_priorities",
        "title": "inclusive_mobility_improvement_priorities",
        "timeFieldName": "weather_date",
    },
]


def _api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{KIBANA_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {
        "Content-Type": "application/json",
        "kbn-xsrf": "true",
    }
    req = Request(url, data=data, headers=headers, method=method)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def wait_for_kibana(timeout: int = 120) -> None:
    """Block until Kibana API is ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = _api("GET", "/api/status")
            if resp.get("status", {}).get("overall", {}).get("level") == "available":
                print("[Kibana] Ready.")
                return
        except (URLError, OSError):
            pass
        time.sleep(3)
    raise TimeoutError("Kibana did not become ready")


def create_index_patterns() -> None:
    """Create data views for all inclusive mobility indices."""
    wait_for_kibana()

    for pattern in INDEX_PATTERNS:
        print(f"[Kibana] Creating data view: {pattern['title']}")
        try:
            _api("POST", "/api/data_views/data_view", {
                "data_view": {
                    "id": pattern["id"],
                    "title": pattern["title"],
                    "timeFieldName": pattern["timeFieldName"],
                }
            })
            print(f"  Created.")
        except Exception as e:
            if "already exists" in str(e) or "409" in str(e):
                print(f"  Already exists.")
            else:
                print(f"  Error: {e}")


def setup_kibana_dashboards(**kwargs) -> str:
    """Airflow-callable entry point for Kibana setup."""
    print("Setting up Kibana index patterns...")
    create_index_patterns()
    print(f"Kibana dashboards ready at {KIBANA_URL}")
    return KIBANA_URL


if __name__ == "__main__":
    create_index_patterns()
