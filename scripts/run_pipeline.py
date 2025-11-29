"""
scripts/run_pipeline.py
The 'Cinematic' Driver for the SalesOps Suite.
Executes the full pipeline and stages artifacts for the Dashboard.
Automatically manages the Mock Server lifecycle.
"""

import sys
import os
import json
import time
import shutil
import argparse
import logging
import requests
from subprocess import Popen
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


def start_mock_server(port=7777):
    """Starts the mock server in a background process."""
    logger.info(f"🚀 Starting Mock Enterprise Server on port {port}...")

    root_dir = Path(__file__).resolve().parent.parent
    log_file = open(root_dir / "outputs" / "mock_server_pipeline.log", "w")

    # Ensure outputs dir exists
    (root_dir / "outputs").mkdir(exist_ok=True)

    process = Popen(
        [sys.executable, "-m", "uvicorn", "tools.mock_server:app", "--port", str(port)],
        stdout=log_file,
        stderr=log_file,
        cwd=root_dir,
    )

    # Wait for health check
    retries = 5
    while retries > 0:
        try:
            requests.get(f"http://localhost:{port}/health")
            logger.info("✅ Mock Server is Ready.")
            return process, log_file
        except requests.ConnectionError:
            time.sleep(1)
            retries -= 1

    logger.error("❌ Failed to start Mock Server.")
    process.terminate()
    return None, log_file


def run_integration_demo(
    data_path: str, output_dir: str, workers: int = 3, dry_run: bool = False
):
    """
    Executes the end-to-end flow and exports to a stable 'demo' folder.
    """
    start_time = time.time()

    # 0. Setup Paths
    root_dir = Path(__file__).resolve().parent.parent
    demo_output = root_dir / output_dir
    dashboard_data = root_dir / "dashboard_data"

    # Clean previous demo artifacts
    if demo_output.exists():
        shutil.rmtree(demo_output)
    demo_output.mkdir(parents=True, exist_ok=True)
    dashboard_data.mkdir(parents=True, exist_ok=True)

    print(f"🎬 Starting Integration Demo")
    print(f"   Input: {data_path}")
    print(f"   Output: {demo_output}")

    # 0. Seed Memory Bank (Ensure file exists for Dashboard)
    print("🌱 Seeding Memory Bank...")
    try:
        mem = MemoryAgent()
        # Only seed if empty
        if mem.bank.backend.count() == 0:
            mem.bank.upsert(
                "Historical: Technology sales dipped in 2014 due to supply chain.", 
                {"type": "resolution", "entity": "Technology"}
            )
            mem.bank.save()
            print("   - Injected seed memory.")
        else:
            print("   - Memory Bank already exists.")
    except Exception as e:
        print(f"   ⚠️ Memory seed failed: {e}")

    # 2. Start Server (if not dry run)
    server_proc = None
    server_log = None

    if not dry_run:
        try:
            # Check if running
            requests.get("http://localhost:7777/health")
        except:
            server_proc, server_log = start_mock_server()
            if not server_proc:
                return False

    try:
        # 3. Initialize Coordinator
        coordinator = A2ACoordinator(
            output_dir=str(demo_output.parent), dry_run=dry_run
        )

        # 4. Configure Flow
        flow_config = {
            "id": "integration_demo",
            "parallelism": workers,
            "confirm_actions": not dry_run,
            "max_anomalies": 5,
        }

        inputs = {"csv_path": data_path}
        session_id = "session:live-demo"

        # 5. Execute
        manifest = coordinator.run(flow_config, inputs, session_id)

        if manifest["status"] != "completed":
            print(f"❌ Pipeline Failed: {manifest.get('error')}")
            return False

        # 6. Export Artifacts
        print("\n📦 Exporting Artifacts to 'dashboard_data/'...")
        artifacts = manifest.get("artifacts", {})

        def safe_copy(key, dest_name):
            src = artifacts.get(key)
            if src and os.path.exists(src):
                shutil.copy(src, dashboard_data / dest_name)
                print(f"   - {dest_name}")

        safe_copy("snapshot", "snapshot.parquet")
        safe_copy("anomalies", "anomalies.json")
        safe_copy("explanations", "enriched.json")
        safe_copy("actions_log", "actions.jsonl")

        with open(dashboard_data / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"   - manifest.json")

        print(f"\n✨ Demo Complete in {time.time() - start_time:.2f}s")
        return True

    except Exception as e:
        logger.exception("Critical Failure in Pipeline Driver")
        return False
    finally:
        # Cleanup Server
        if server_proc:
            logger.info("🛑 Shutting down Mock Server...")
            server_proc.terminate()
            server_log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/raw/superstore.csv")
    parser.add_argument("--out", default="outputs/demo_run")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    success = run_integration_demo(args.data, args.out, args.workers, args.dry_run)
    sys.exit(0 if success else 1)
