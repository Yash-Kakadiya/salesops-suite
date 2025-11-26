"""
evaluation/impact_simulator.py
Estimates the business value (ROI) of the SalesOps Agent.
"""

import json
import pandas as pd
import argparse
from pathlib import Path


def calculate_impact(run_dir: str, recovery_rate: float = 0.30):
    print(f"💰 Calculating Business Impact (Recovery Rate: {recovery_rate*100}%)")

    # Load Explainer Output (Enriched Anomalies)
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
        # Logic: We can only "recover" lost revenue from dips/drops.
        # Spikes are "found money" or inventory risks, calculated differently.

        val = rec.get("value", 0)
        exp = rec.get("expected", 0)

        # If Value < Expected, it's a drop (Loss)
        if val < exp:
            loss = exp - val
            recovery = loss * recovery_rate

            total_revenue_at_risk += loss
            recoverable_revenue += recovery

            impact_log.append(
                {
                    "id": rec.get("entity_id"),
                    "loss": loss,
                    "recovered": recovery,
                    "action": rec.get("suggested_actions", []),
                }
            )

        if rec.get("suggested_actions"):
            actions_triggered += 1

    results = {
        "metric": "Business Impact",
        "anomalies_processed": len(anomalies),
        "negative_events": len(impact_log),
        "total_revenue_at_risk": round(total_revenue_at_risk, 2),
        "estimated_annual_recovery": round(recoverable_revenue * 12, 2),  # Annualized
        "one_time_recovery": round(recoverable_revenue, 2),
        "actions_automated": actions_triggered,
    }

    return results


if __name__ == "__main__":
    # Auto-detect latest run
    import glob
    import os

    base_dir = Path("../outputs/runs")

    try:
        runs = sorted(
            [d for d in base_dir.iterdir() if d.is_dir()], key=os.path.getmtime
        )
        latest = runs[-1]

        impact = calculate_impact(str(latest))
        print(json.dumps(impact, indent=2))

        with open("results_impact.json", "w") as f:
            json.dump(impact, f, indent=2)

    except Exception as e:
        print(f"Impact calculation failed: {e}")
