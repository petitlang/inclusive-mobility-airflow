from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from kafka.common import compact_json, run_command
from lib.config import (
    DEFAULT_CITY,
    DEFAULT_LATITUDE,
    DEFAULT_LONGITUDE,
    KAFKA_BOOTSTRAP_SERVER,
    OPEN_METEO_API_URL,
    OPEN_METEO_CURRENT_VARIABLES,
    OPEN_METEO_TIMEZONE,
    WEATHER_STREAM_TOPIC,
)


def build_open_meteo_current_url(
    latitude: float = DEFAULT_LATITUDE,
    longitude: float = DEFAULT_LONGITUDE,
    timezone_name: str = OPEN_METEO_TIMEZONE,
) -> str:
    """Build the Open-Meteo current weather URL.

    Args:
        latitude: Weather latitude.
        longitude: Weather longitude.
        timezone_name: IANA timezone requested from Open-Meteo.

    Returns:
        Fully qualified HTTPS URL for current weather values.
    """
    query = urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(OPEN_METEO_CURRENT_VARIABLES),
            "timezone": timezone_name,
        }
    )
    return f"{OPEN_METEO_API_URL}?{query}"


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


def build_weather_event() -> dict[str, Any]:
    """Fetch Open-Meteo current weather and wrap it as a Kafka event.

    Returns:
        Event dictionary containing metadata, request URL and raw API response.
    """
    request_url = build_open_meteo_current_url()
    response = request_json(request_url)
    return {
        "event_type": "open_meteo_current_weather",
        "source": "open_meteo",
        "city": DEFAULT_CITY,
        "latitude": DEFAULT_LATITUDE,
        "longitude": DEFAULT_LONGITUDE,
        "timezone": OPEN_METEO_TIMEZONE,
        "request_url": request_url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "response": response,
    }


def create_topic(topic: str, bootstrap_server: str) -> None:
    """Create a Kafka topic if it does not already exist.

    Args:
        topic: Kafka topic name.
        bootstrap_server: Kafka bootstrap server reachable from the Kafka container.

    Side effects:
        Executes `kafka-topics --create --if-not-exists` in the Kafka container.
    """
    run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ]
    )


def publish_event(topic: str, bootstrap_server: str, event: dict[str, Any]) -> None:
    """Publish one JSON event to Kafka.

    Args:
        topic: Kafka topic name.
        bootstrap_server: Kafka bootstrap server reachable from the Kafka container.
        event: JSON-serializable event payload.

    Side effects:
        Writes one compact JSON line to `kafka-console-producer`.
    """
    run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            bootstrap_server,
            "--topic",
            topic,
        ],
        input_text=compact_json(event) + "\n",
    )


def main() -> None:
    """CLI entry point for one-shot Open-Meteo current weather publishing."""
    parser = argparse.ArgumentParser(description="Publish Open-Meteo current weather to Kafka.")
    parser.add_argument("--topic", default=WEATHER_STREAM_TOPIC)
    parser.add_argument("--bootstrap-server", default=KAFKA_BOOTSTRAP_SERVER)
    args = parser.parse_args()

    event = build_weather_event()
    create_topic(args.topic, args.bootstrap_server)
    publish_event(args.topic, args.bootstrap_server, event)
    print(compact_json({"topic": args.topic, "event_type": event["event_type"], "status": "published"}))


if __name__ == "__main__":
    main()
