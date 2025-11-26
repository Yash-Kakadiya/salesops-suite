"""
evaluation/eval_schema_compliance.py
Verifies that LLM outputs conform to the strict JSON schema (Robustness).
"""

import json
import argparse
from pathlib import Path

REQUIRED_KEYS = [
    "explanation_short",
    "explanation_full",
    "suggested_actions",
    "confidence",
    "needs_human_review",
]


def evaluate_schema(json_path: str) -> dict:
    print(f"🔍 Checking schema compliance for: {json_path}")

    if not Path(json_path).exists():
        return {"error": "File not found"}

    with open(json_path, "r") as f:
        data = json.load(f)

    total = len(data)
    valid = 0
    errors = []

    for item in data:
        missing = [k for k in REQUIRED_KEYS if k not in item]
        if not missing:
            valid += 1
        else:
            errors.append(f"ID {item.get('anomaly_id')}: Missing {missing}")

    score = valid / total if total > 0 else 0

    results = {
        "metric": "LLM Schema Compliance",
        "total_records": total,
        "valid_records": valid,
        "compliance_rate": round(score, 2),
        "errors": errors[:5],  # Top 5
    }
    return results


if __name__ == "__main__":
    # Default to the output from Day 7/8 run
    # Find the latest run dir
    import glob

    runs = sorted(glob.glob("../outputs/runs/*"))
    latest_file = f"{runs[-1]}/enriched_anomalies.json" if runs else "dummy"

    res = evaluate_schema(latest_file)
    print(json.dumps(res, indent=2))

    with open("./results_schema.json", "w") as f:
        json.dump(res, f)
