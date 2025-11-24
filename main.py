"""
SalesOps Suite - Main Entry Point
Usage: python main.py --data data/raw/superstore.csv --output-dir outputs/prod
"""

import argparse
import os
import uuid
from agents.a2a_coordinator import A2ACoordinator


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

    print(
        f"🚀 Starting SalesOps Suite (Workers: {args.workers}, Output: {args.output_dir})..."
    )
    if args.dry_run:
        print("   [DRY RUN MODE ENABLED]")

    # 1. Initialize Coordinator with CLI Args
    coordinator = A2ACoordinator(output_dir=args.output_dir, dry_run=args.dry_run)

    # 2. Configure Flow
    flow_config = {
        "id": "daily_full_run",
        "confirm_actions": not args.dry_run,
        "parallelism": args.workers,
    }

    inputs = {"csv_path": args.data}

    session_id = f"cli-session-{uuid.uuid4().hex[:8]}"

    # 3. Execute Flow
    manifest = coordinator.run(flow_config, inputs, session_id)

    # 4. Report Status
    log_path = coordinator.master_manifest_path

    if manifest["status"] == "completed":
        print("\n✅ Pipeline Finished Successfully.")
        print(f"   Run ID: {manifest['run_id']}")
        print(f"   Artifacts: {list(manifest.get('artifacts', {}).keys())}")
    else:
        print(f"\n❌ Pipeline Failed: {manifest.get('error')}")
        print(f"   Check logs in: {log_path}")


if __name__ == "__main__":
    main()
