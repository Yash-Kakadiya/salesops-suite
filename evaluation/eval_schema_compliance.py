"""
evaluation/eval_schema_compliance.py
Verifies that LLM outputs conform to the strict JSON schema.
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
        "errors": errors[:5],
    }
    return results


if __name__ == "__main__":
    import glob

    # Find relative to parent root
    root = Path(__file__).parents[1]
    runs_dir = root / "outputs" / "runs"

    runs = sorted(glob.glob(str(runs_dir / "run_*")))
    latest_file = f"{runs[-1]}/enriched_anomalies.json" if runs else "dummy"

    # Override if arg provided
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=latest_file)
    args = parser.parse_args()

    res = evaluate_schema(args.file)
    print(json.dumps(res, indent=2))

    output_path = Path(__file__).parent / "results_schema.json"
    with open(output_path, "w") as f:
        json.dump(res, f, indent=2)
    print(f"✅ Saved to {output_path}")
