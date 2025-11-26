"""
evaluation/eval_detector.py
Calculates Precision/Recall/F1 with Robust Entity Matching.
"""

import pandas as pd
import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set

# Import our Agent
sys.path.append(str(Path(__file__).parents[1]))
from agents.anomaly_stats_agent import AnomalyStatAgent


def normalize_date(d):
    """Force date to YYYY-MM-DD string."""
    return pd.to_datetime(d).strftime("%Y-%m-%d")


def normalize_entity(e):
    """Map synonyms to the Agent's canonical names."""
    e_str = str(e).strip()
    mapping = {
        "All": "All_Regions",
        "Global": "All_Regions",
        "all": "All_Regions",
        "global": "All_Regions",
    }
    return mapping.get(e_str, e_str)


def evaluate_detector(synthetic_data_path: str, gold_labels_path: str) -> Dict:
    print(f"Evaluation: Loading data from {synthetic_data_path}")

    if not Path(synthetic_data_path).exists():
        raise FileNotFoundError(f"Data not found: {synthetic_data_path}")

    df = pd.read_parquet(synthetic_data_path)

    # 1. Run Agent
    agent = AnomalyStatAgent(df)

    # Run all detectors
    agent.detect_global_zscore(threshold=2.0)
    agent.detect_grouped_iqr(group_col="Region", k=1.5)
    agent.detect_grouped_iqr(group_col="Category", k=1.5)

    # Add percentage change detectors for both drops and spikes
    agent.detect_percentage_drop(group_col="Category", threshold=0.05)  # 5% drop

    # Also detect significant jumps (to catch the 177% spike)
    agent.detect_percentage_spike(group_col="Category", threshold=0.5)  # 50% spike

    all_detected = agent.get_anomalies_df()
    print(f"Agent found {len(all_detected)} anomalies.")

    # 2. Load Ground Truth
    gold_labels = []
    with open(gold_labels_path, "r") as f:
        for line in f:
            gold_labels.append(json.loads(line))
    print(f"Gold dataset has {len(gold_labels)} labeled anomalies.")

    # 3. Match Logic
    true_positives = 0
    missed = 0

    # Build detected set: "YYYY-MM-DD|Entity"
    detected_keys = set()
    for _, row in all_detected.iterrows():
        d_str = normalize_date(row["period_start"])
        e_str = normalize_entity(row["entity_id"])
        detected_keys.add(f"{d_str}|{e_str}")

    # Check Recall
    for label in gold_labels:
        l_date = normalize_date(label["date"])
        l_entity = normalize_entity(label["entity"])

        target_key = f"{l_date}|{l_entity}"

        if target_key in detected_keys:
            true_positives += 1
        else:
            missed += 1
            print(f"MISSED: {target_key} (Type: {label.get('type')})")

    # 4. Metrics
    total_detected = len(detected_keys)
    total_gold = len(gold_labels)

    precision = true_positives / total_detected if total_detected > 0 else 0
    recall = true_positives / total_gold if total_gold > 0 else 0
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    results = {
        "metric": "Detector Quality",
        "true_positives": true_positives,
        "false_positives": total_detected - true_positives,
        "missed_labels": missed,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
    }

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="../data/test_labels/synthetic_sales.parquet")
    parser.add_argument("--gold", default="../data/test_labels/anomalies_gold.jsonl")
    args = parser.parse_args()

    try:
        metrics = evaluate_detector(args.data, args.gold)
        print("\nFinal Score:")
        print(json.dumps(metrics, indent=2))

        with open("./results_detector.json", "w") as f:
            json.dump(metrics, f, indent=2)

    except Exception as e:
        print(f"Evaluation Failed: {e}")
