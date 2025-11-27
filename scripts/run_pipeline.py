"""
scripts/run_pipeline.py
The 'Cinematic' Driver for the SalesOps Suite.
Executes the full pipeline and stages artifacts for the Dashboard.
"""

import sys
import os
import json
import time
import shutil
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

from agents.a2a_coordinator import A2ACoordinator
from agents.memory_agent import MemoryAgent

# Configure pretty logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PipelineDriver")


def run_integration_demo(
    data_path: str, output_dir: str, workers: int = 3, dry_run: bool = False
):
    """
    Executes the end-to-end flow and exports to a stable 'demo' folder.
    """
    start_time = time.time()

    # 1. Setup Paths
    root_dir = Path(__file__).resolve().parent.parent
    demo_output = root_dir / output_dir
    dashboard_data = root_dir / "dashboard_data"

    # Clean previous demo artifacts to ensure fresh results
    if demo_output.exists():
        shutil.rmtree(demo_output)
    demo_output.mkdir(parents=True, exist_ok=True)

    dashboard_data.mkdir(parents=True, exist_ok=True)

    print(f"🎬 Starting Integration Demo")
    print(f"   Input: {data_path}")
    print(f"   Output: {demo_output}")
    print(f"   Workers: {workers} | Dry Run: {dry_run}")

    # 2. Initialize Coordinator
    # We point the coordinator's base output to our demo folder
    coordinator = A2ACoordinator(output_dir=str(demo_output.parent), dry_run=dry_run)

    # Force the specific run directory to be 'outputs/demo_run' (or similar)
    # for predictable dashboard loading, OR we rely on the export step.
    # A2ACoordinator generates timestamped runs. We will copy artifacts later.

    # 3. Configure Flow
    flow_config = {
        "id": "integration_demo",
        "parallelism": workers,
        "confirm_actions": not dry_run,
        "max_anomalies": 5,
    }

    inputs = {"csv_path": data_path}

    session_id = "session:live-demo"

    # 4. Execute
    try:
        manifest = coordinator.run(flow_config, inputs, session_id)

        if manifest["status"] != "completed":
            print(f"❌ Pipeline Failed: {manifest.get('error')}")
            return False

        # 5. Export Artifacts for Dashboard
        # The Dashboard expects files in 'dashboard_data/' for fast loading
        print("\n📦 Exporting Artifacts to 'dashboard_data/'...")

        artifacts = manifest.get("artifacts", {})

        # Copy helper
        def safe_copy(key, dest_name):
            src = artifacts.get(key)
            if src and os.path.exists(src):
                shutil.copy(src, dashboard_data / dest_name)
                print(f"   - {dest_name}")
            else:
                print(f"   ⚠️ Missing artifact: {key}")

        safe_copy("snapshot", "snapshot.parquet")
        safe_copy("anomalies", "anomalies.json")
        safe_copy("explanations", "enriched.json")
        safe_copy("actions_log", "actions.jsonl")

        # Save manifest
        with open(dashboard_data / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"   - manifest.json")

        print(f"\n✨ Demo Complete in {time.time() - start_time:.2f}s")
        return True

    except Exception as e:
        logger.exception("Critical Failure in Pipeline Driver")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the SalesOps Integration Demo")
    parser.add_argument("--data", default="data/raw/superstore.csv", help="Input CSV")
    parser.add_argument("--out", default="outputs/demo_run", help="Output directory")
    parser.add_argument("--workers", type=int, default=3, help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Skip external actions")

    args = parser.parse_args()

    success = run_integration_demo(args.data, args.out, args.workers, args.dry_run)
    sys.exit(0 if success else 1)
