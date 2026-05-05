from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "dags"))

from kafka.common import run_command
from lib.config import KAFKA_BOOTSTRAP_SERVER, WEATHER_STREAM_TOPIC


def raw_stream_file(day: str | None = None) -> Path:
    partition_day = day or date.today().strftime("%Y%m%d")
    return (
        PROJECT_ROOT
        / "datalake"
        / "raw"
        / "open_meteo"
        / "current_weather_stream"
        / partition_day
        / "events.jsonl"
    )


def consume_messages(topic: str, bootstrap_server: str, max_messages: int) -> list[str]:
    result = run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap_server,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            str(max_messages),
            "--timeout-ms",
            "30000",
        ]
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def write_messages(messages: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as handle:
        for message in messages:
            handle.write(message)
            handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist Kafka weather events to raw JSONL.")
    parser.add_argument("--topic", default=WEATHER_STREAM_TOPIC)
    parser.add_argument("--bootstrap-server", default=KAFKA_BOOTSTRAP_SERVER)
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--output-file", type=Path, default=None)
    args = parser.parse_args()

    messages = consume_messages(args.topic, args.bootstrap_server, args.max_messages)
    output_file = args.output_file or raw_stream_file()
    write_messages(messages, output_file)
    print(f"Wrote {len(messages)} Kafka weather event(s) to {output_file}")


if __name__ == "__main__":
    main()
