"""
evaluation/run_all.py
Master script to execute the full Evaluation Pipeline.
Fix: Uses Relative Paths to avoid issues with spaces in parent directories.
"""

import os
import sys
import subprocess
from pathlib import Path


def run(cmd):
    print(f"▶️ Running: {cmd}")
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        print(f"❌ Failed: {cmd}")
        sys.exit(1)


# 1. Setup Context
# Force the Current Working Directory to the Project Root
# This allows us to use clean relative paths (e.g. "data/processed")
ROOT_DIR = Path(__file__).resolve().parent.parent
os.chdir(ROOT_DIR)
print(f"📂 Working Directory set to Project Root: {os.getcwd()}")

# 2. Define Relative Paths
DATA_CLEAN = Path("data/processed/superstore_clean.parquet")
DATA_TEST = Path("data/test_labels/synthetic_sales.parquet")
LABELS_GOLD = Path("data/test_labels/anomalies_gold.jsonl")
TEST_LABELS_DIR = Path("data/test_labels")

# Scripts (Relative)
SCRIPT_GEN = Path("evaluation/create_synthetic_anomalies.py")
SCRIPT_DET = Path("evaluation/eval_detector.py")
SCRIPT_SCHEMA = Path("evaluation/eval_schema_compliance.py")
SCRIPT_IMPACT = Path("evaluation/impact_simulator.py")

# 3. Verify Pre-requisites
if not DATA_CLEAN.exists():
    print(f"⚠️ Warning: {DATA_CLEAN} not found.")
    # Check outputs/runs for a fallback snapshot
    runs_dir = Path("outputs/runs")
    if runs_dir.exists():
        runs = sorted(
            [d for d in runs_dir.iterdir() if d.is_dir()], key=os.path.getmtime
        )
        if runs:
            latest_snap = runs[-1] / "snapshot.parquet"
            if latest_snap.exists():
                print(f"   Found fallback snapshot: {latest_snap}")
                DATA_CLEAN = latest_snap

if not DATA_CLEAN.exists():
    print("❌ Error: No clean data found. Run 'main.py' first.")
    sys.exit(1)

print("🚀 Starting Evaluation Pipeline...")

# 4. Execute Pipeline

# Step 1: Data Generation
# Note: We use str() to pass clean relative paths to the command
run(f"python {SCRIPT_GEN} --input {DATA_CLEAN} --out {TEST_LABELS_DIR}")

# Step 2: Detector Eval
run(f"python {SCRIPT_DET} --data {DATA_TEST} --gold {LABELS_GOLD}")

# Step 3: Schema Eval
# Find latest run for enriched anomalies
runs_dir = Path("outputs/runs")
if runs_dir.exists():
    runs = sorted([d for d in runs_dir.iterdir() if d.is_dir()], key=os.path.getmtime)
    if runs:
        latest_enriched = runs[-1] / "enriched_anomalies.json"
        if latest_enriched.exists():
            run(f"python {SCRIPT_SCHEMA} --file {latest_enriched}")
        else:
            print("⚠️ No enriched anomalies found in latest run.")
    else:
        print("⚠️ No runs found.")

# Step 4: Impact Eval
run(f"python {SCRIPT_IMPACT}")

print(
    "\n✅ Evaluation Complete. Open 'evaluation/99_evaluation_report.ipynb' to view the scorecard."
)
