from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_command(command: list[str], input_text: str | None = None) -> subprocess.CompletedProcess:
    """Run a subprocess command used by Kafka helper scripts.

    Args:
        command: Command and arguments to execute.
        input_text: Optional stdin content passed to the subprocess.

    Returns:
        Completed subprocess result with captured stdout and stderr.
    """
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )


def compact_json(payload: dict[str, Any]) -> str:
    """Serialize a JSON object as one compact line for Kafka messages.

    Args:
        payload: JSON-serializable dictionary.

    Returns:
        UTF-8 friendly compact JSON string without extra whitespace.
    """
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
