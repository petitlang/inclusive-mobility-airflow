from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_python(script: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, str(script), *args], cwd=PROJECT_ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Open-Meteo API -> Kafka -> raw JSONL.")
    parser.add_argument("--max-messages", type=int, default=1)
    args = parser.parse_args()

    producer = PROJECT_ROOT / "kafka" / "producers" / "open_meteo_current_producer.py"
    consumer = PROJECT_ROOT / "kafka" / "consumers" / "weather_stream_to_raw.py"

    run_python(producer, [])
    run_python(consumer, ["--max-messages", str(args.max_messages)])


if __name__ == "__main__":
    main()
