"""
SalesOps Suite - Main Entry Point
Usage: python main.py --data data/raw/superstore.csv --output-dir outputs/prod
"""

import argparse
import os
import sys
import time
import uuid
import requests
import signal
from subprocess import Popen
from agents.a2a_coordinator import A2ACoordinator


def start_mock_server(port=7777):
    """Starts the mock server in a background process."""
    print(f"🚀 Starting Mock Enterprise Server on port {port}...")
    log_file = open("outputs/mock_server_main.log", "w")

    # Ensure outputs dir exists
    os.makedirs("outputs", exist_ok=True)

    process = Popen(
        [sys.executable, "-m", "uvicorn", "tools.mock_server:app", "--port", str(port)],
        stdout=log_file,
        stderr=log_file,
    )

    # Wait for health check
    retries = 5
    while retries > 0:
        try:
            requests.get(f"http://localhost:{port}/health")
            print("✅ Mock Server is Ready.")
            return process, log_file
        except requests.ConnectionError:
            time.sleep(1)
            retries -= 1

    print("❌ Failed to start Mock Server. Check outputs/mock_server_main.log")
    process.terminate()
    return None, log_file


def main():
    parser = argparse.ArgumentParser(description="SalesOps Autonomous Agent Suite")
    parser.add_argument(
        "--data", default="data/raw/superstore.csv", help="Path to input CSV"
    )
    parser.add_argument(
        "--workers", type=int, default=3, help="Parallel workers for AI explanations"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run without sending real emails/tickets"
    )
    parser.add_argument(
        "--output-dir", default="outputs", help="Directory for artifacts and logs"
    )

    args = parser.parse_args()

    if not os.path.exists(args.data):
        print(f"❌ Error: Data file not found at {args.data}")
        return

    # 1. Start Infrastructure
    server_proc, server_log = start_mock_server(port=7777)
    if not server_proc:
        return

    try:
        print(
            f"🚀 Starting SalesOps Suite (Workers: {args.workers}, Output: {args.output_dir})..."
        )
        if args.dry_run:
            print("   [DRY RUN MODE ENABLED]")

        # 2. Initialize Coordinator
        coordinator = A2ACoordinator(output_dir=args.output_dir, dry_run=args.dry_run)

        # 3. Configure Flow
        flow_config = {
            "id": "daily_full_run",
            "confirm_actions": not args.dry_run,
            "parallelism": args.workers,
        }

        inputs = {"csv_path": args.data}

        session_id = f"cli-session-{uuid.uuid4().hex[:8]}"

        # 4. Execute Flow
        manifest = coordinator.run(flow_config, inputs, session_id)

        # 5. Report Status
        log_path = coordinator.master_manifest_path

        if manifest["status"] == "completed":
            print("\n✅ Pipeline Finished Successfully.")
            print(f"   Run ID: {manifest['run_id']}")
            print(f"   Artifacts: {list(manifest.get('artifacts', {}).keys())}")
        else:
            print(f"\n❌ Pipeline Failed: {manifest.get('error')}")
            print(f"   Check logs in: {log_path}")

    finally:
        # Cleanup Server
        print("🛑 Shutting down Mock Server...")
        server_proc.terminate()
        server_log.close()


if __name__ == "__main__":
    main()
