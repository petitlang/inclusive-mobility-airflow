from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from lib.config import (
    ACCES_LIBRE_API_URL,
    ACCES_LIBRE_MAX_PAGES,
    ACCES_LIBRE_PAGE_SIZE,
    ACCES_LIBRE_TABULAR_API_URL,
)
from lib.paths import ensure_datalake_dir


def build_acces_libre_url(page: int = 1, page_size: int = ACCES_LIBRE_PAGE_SIZE) -> str:
    query = urlencode({"page": page, "page_size": page_size})
    return f"{ACCES_LIBRE_TABULAR_API_URL}?{query}"


def request_json(url: str, timeout: int = 30) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def fetch_acces_libre_pages(
    page_size: int = ACCES_LIBRE_PAGE_SIZE,
    max_pages: int = ACCES_LIBRE_MAX_PAGES,
) -> list[dict[str, Any]]:
    pages = []
    for page in range(1, max_pages + 1):
        url = build_acces_libre_url(page=page, page_size=page_size)
        response = request_json(url)
        pages.append({"page": page, "url": url, "response": response})

        if not response.get("links", {}).get("next"):
            break

    return pages


def fetch_accessibility_data(**kwargs) -> str:
    """Fetch a raw AccesLibre sample through the public data.gouv tabular API."""
    target_dir = ensure_datalake_dir("raw", "acces_libre", "establishments")
    target_file = target_dir / "accessibility.json"
    pages = fetch_acces_libre_pages()
    records = [
        record
        for page in pages
        for record in page.get("response", {}).get("data", [])
    ]
    payload = {
        "source": "acces_libre",
        "source_note": (
            "The official AccesLibre endpoint is documented but requires an API key "
            "for direct calls. This stage ingests the associated public data.gouv "
            "tabular REST resource so the pipeline can run without secrets."
        ),
        "official_api_url": ACCES_LIBRE_API_URL,
        "ingestion_api_url": ACCES_LIBRE_TABULAR_API_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "page_size": ACCES_LIBRE_PAGE_SIZE,
        "max_pages": ACCES_LIBRE_MAX_PAGES,
        "record_count": len(records),
        "pages": pages,
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(records)} raw AccesLibre records to: {target_file}")
    return str(target_file)
