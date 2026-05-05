from __future__ import annotations

import json

from lib.paths import ensure_datalake_dir


def calculate_mobility_score(
    accessibility_score: float,
    weather_risk_score: float,
) -> float:
    """Combine accessibility quality and weather risk into a 0-100 score."""
    score = accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3
    return round(max(0, min(100, score)), 2)


def compute_daily_mobility_scores(**kwargs) -> str:
    """Create the usage output folder for daily mobility scores."""
    target_dir = ensure_datalake_dir("usage", "inclusive_mobility", "mobility_scores")
    target_file = target_dir / "scores.json"
    payload = {
        "status": "placeholder",
        "formula": "accessibility_score * 0.7 + (100 - weather_risk_score) * 0.3",
        "example_score": calculate_mobility_score(80, 30),
    }
    target_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Prepared usage mobility score file: {target_file}")
    return str(target_file)
