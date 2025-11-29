"""
evaluation/impact_simulator.py
Estimates the business value (ROI) of the SalesOps Agent.
"""

import json
import pandas as pd
import argparse
import os
from pathlib import Path


def calculate_impact(run_dir: str, recovery_rate: float = 0.30):
    print(f"💰 Calculating Business Impact (Recovery Rate: {recovery_rate*100}%)")

    input_path = Path(run_dir) / "enriched_anomalies.json"
    if not input_path.exists():
        return {"error": "No enriched anomalies found"}

    with open(input_path, "r") as f:
        anomalies = json.load(f)

    total_revenue_at_risk = 0.0
    recoverable_revenue = 0.0
    actions_triggered = 0

    impact_log = []

    for rec in anomalies:
        val = rec.get("value", 0)
        exp = rec.get("expected", 0)

        if val < exp:
            loss = exp - val
            recovery = loss * recovery_rate

            total_revenue_at_risk += loss
            recoverable_revenue += recovery

            impact_log.append(
                {"id": rec.get("entity_id"), "loss": loss, "recovered": recovery}
            )

        if rec.get("suggested_actions"):
            actions_triggered += 1

    results = {
        "metric": "Business Impact",
        "anomalies_processed": len(anomalies),
        "negative_events": len(impact_log),
        "total_revenue_at_risk": round(total_revenue_at_risk, 2),
        "estimated_annual_recovery": round(recoverable_revenue * 12, 2),
        "one_time_recovery": round(recoverable_revenue, 2),
        "actions_automated": actions_triggered,
    }
    return results


if __name__ == "__main__":
    # Resolve Root relative to script
    root_dir = Path(__file__).resolve().parents[1]
    base_dir = root_dir / "outputs" / "runs"

    try:
        if not base_dir.exists():
            # Fallback if running inside evaluation folder manually
            base_dir = Path("../outputs/runs")

        runs = sorted(
            [d for d in base_dir.iterdir() if d.is_dir()], key=os.path.getmtime
        )
        if not runs:
            print("No runs found.")
            exit(0)

        latest = runs[-1]
        impact = calculate_impact(str(latest))
        print(json.dumps(impact, indent=2))

        output_path = Path(__file__).parent / "results_impact.json"
        with open(output_path, "w") as f:
            json.dump(impact, f, indent=2)
        print(f"✅ Saved to {output_path}")

    except Exception as e:
        print(f"Impact calculation failed: {e}")
