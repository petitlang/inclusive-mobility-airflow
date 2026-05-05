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
    """Build the public data.gouv tabular API URL for AccesLibre records.

    Args:
        page: One-based page number to request.
        page_size: Number of records requested for the page.

    Returns:
        Fully qualified HTTPS URL with pagination query parameters.
    """
    query = urlencode({"page": page, "page_size": page_size})
    return f"{ACCES_LIBRE_TABULAR_API_URL}?{query}"


def request_json(url: str, timeout: int = 30) -> dict[str, Any]:
    """Request JSON from an HTTP endpoint.

    Args:
        url: HTTP or HTTPS URL to call.
        timeout: Network timeout in seconds.

    Returns:
        Decoded JSON object as a dictionary.
    """
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def fetch_acces_libre_pages(
    page_size: int = ACCES_LIBRE_PAGE_SIZE,
    max_pages: int = ACCES_LIBRE_MAX_PAGES,
) -> list[dict[str, Any]]:
    """Fetch a bounded set of AccesLibre tabular API pages.

    Args:
        page_size: Number of records requested per API page.
        max_pages: Maximum number of pages to fetch.

    Returns:
        A list of page envelopes containing the page number, request URL and raw
        API response.
    """
    pages = []
    for page in range(1, max_pages + 1):
        url = build_acces_libre_url(page=page, page_size=page_size)
        response = request_json(url)
        pages.append({"page": page, "url": url, "response": response})

        if not response.get("links", {}).get("next"):
            break

    return pages


def fetch_accessibility_data(**kwargs) -> str:
    """Fetch AccesLibre raw records and write them to the Data Lake.

    Args:
        **kwargs: Airflow context arguments, currently unused.

    Returns:
        String path to the raw JSON file written.

    Side effects:
        Creates `datalake/raw/acces_libre/establishments/YYYYMMDD/accessibility.json`.
    """
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
