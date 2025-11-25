"""
agents/a2a_coordinator.py
Production Hardened A2A Coordinator.
Features: Atomic Writes, Stale Lock Handling, Dry-Run Propagation, Robust Observability.
"""

import os
import json
import uuid
import logging
import time
import random
import threading
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

# Observability Imports
from observability.logger import timeit_span, get_logger
from observability.metrics import RUNS_TOTAL

# Import Agents
from agents.data_ingestor import DataIngestorAgent
from agents.anomaly_stats_agent import AnomalyStatAgent
from agents.anomaly_llm_agent import AnomalyExplainerAgent
from agents.action_agent import ActionAgent

# Use Structured Logger
logger = get_logger("A2ACoordinator")


@dataclass
class TaskContext:
    run_id: str
    conversation_id: str
    task_id: str
    timeout_seconds: int = 60
    cancellation_token: threading.Event = field(default_factory=threading.Event)
    metadata: Dict[str, Any] = field(default_factory=dict)


class A2ACoordinator:

    def __init__(self, output_dir="../outputs", dry_run=False):
        # ISO 8601 Timestamps for Run ID
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"run_{ts}_{uuid.uuid4().hex[:6]}"
        self.dry_run = dry_run

        # Absolute Paths
        self.output_dir = Path(output_dir).resolve()
        self.run_dir = self.output_dir / "runs" / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.observability_dir = self.output_dir / "observability"
        self.observability_dir.mkdir(parents=True, exist_ok=True)
        self.master_manifest_path = self.observability_dir / "a2a_runs.jsonl"

        # Agents (Propagate Dry Run)
        self.explainer = AnomalyExplainerAgent(dry_run=self.dry_run)
        self.actor = ActionAgent(output_dir=str(self.run_dir))

        # State & Locking
        self._lock = threading.Lock()
        self.task_log = []
        self.artifacts = {}

    def _log_task(self, entry: Dict):
        """Thread-safe logging of task results."""
        with self._lock:
            self.task_log.append(entry)

    def _add_artifact(self, key: str, path: str):
        """Thread-safe artifact tracking."""
        with self._lock:
            self.artifacts[key] = str(path)

    def _append_manifest_atomic(self, manifest: Dict):
        """Atomic append to JSONL with Stale Lock handling."""
        lock_file = self.master_manifest_path.with_suffix(".lock")
        timeout = 5.0
        start = time.time()

        while (time.time() - start) < timeout:
            try:
                if lock_file.exists():
                    lock_age = time.time() - lock_file.stat().st_mtime
                    if lock_age > 10:
                        logger.warning(
                            f"Removing stale lock file (Age: {lock_age:.1f}s)"
                        )
                        os.remove(lock_file)

                with open(lock_file, "x"):
                    with open(self.master_manifest_path, "a") as f:
                        f.write(json.dumps(manifest) + "\n")
                os.remove(lock_file)
                return
            except FileExistsError:
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Manifest write failed: {e}")
                if lock_file.exists():
                    try:
                        os.remove(lock_file)
                    except:
                        pass
                return

        logger.error("Manifest write timed out waiting for lock.")

    def _execute_task(self, func, ctx: TaskContext, *args) -> Any:
        """Executes task with Timeouts, Retries, and detailed logging."""
        retries = ctx.metadata.get("retries", 2)
        base_delay = ctx.metadata.get("retry_delay", 1.0)
        last_err = None

        for attempt in range(retries + 1):
            if ctx.cancellation_token.is_set():
                self._log_task(
                    {"task_id": ctx.task_id, "status": "cancelled", "attempts": attempt}
                )
                return None

            t0 = time.time()

            try:
                # Enforce Timeout
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, *args)
                    result = future.result(timeout=ctx.timeout_seconds)

                duration = round((time.time() - t0) * 1000, 2)

                self._log_task(
                    {
                        "task_id": ctx.task_id,
                        "status": "success",
                        "duration_ms": duration,
                        "attempts": attempt + 1,
                        "start_ts": datetime.fromtimestamp(
                            t0, timezone.utc
                        ).isoformat(),
                        "end_ts": datetime.now(timezone.utc).isoformat(),
                    }
                )
                return result

            except concurrent.futures.TimeoutError:
                last_err = TimeoutError(f"Task exceeded {ctx.timeout_seconds}s")
                logger.warning(f"Task {ctx.task_id} Timed Out.")

            except Exception as e:
                last_err = e
                logger.warning(f"Task {ctx.task_id} Attempt {attempt+1} Failed: {e}")

            if attempt < retries:
                sleep_time = (base_delay * (2**attempt)) + random.uniform(0, 0.5)
                time.sleep(sleep_time)

        self._log_task(
            {
                "task_id": ctx.task_id,
                "status": "failed",
                "error": str(last_err),
                "error_type": type(last_err).__name__,
                "attempts": retries + 1,
                "end_ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        raise last_err

    # --- Task Logic (Instrumented) ---

    @timeit_span("coordinator.ingest")
    def run_ingest(self, csv_path: str) -> str:
        ctx = TaskContext(self.run_id, self.run_id, "Ingestor", timeout_seconds=30)

        def logic(path):
            safe_path = Path(path).resolve()
            if not safe_path.exists():
                raise FileNotFoundError(f"Missing: {safe_path}")

            ingestor = DataIngestorAgent(str(safe_path))
            ingestor.clean_data()
            snap_path = self.run_dir / "snapshot.parquet"
            ingestor.save_snapshot(str(snap_path))
            self._add_artifact("snapshot", str(snap_path))
            return str(snap_path)

        return self._execute_task(logic, ctx, csv_path)

    @timeit_span("coordinator.detect")
    def run_detect(self, snapshot_path: str) -> List[Dict]:
        ctx = TaskContext(self.run_id, self.run_id, "Detector", timeout_seconds=60)

        def logic(path):
            import pandas as pd

            df = pd.read_parquet(path)
            detector = AnomalyStatAgent(df)
            detector.detect_global_zscore()
            detector.detect_grouped_iqr(group_col="Region")
            out_file = self.run_dir / "anomalies.json"
            detector.save_payload(str(out_file))
            self._add_artifact("anomalies", str(out_file))

            with open(out_file) as f:
                return json.load(f).get("top_anomalies", [])

        return self._execute_task(logic, ctx, snapshot_path)

    @timeit_span("coordinator.explain")
    def run_explain(self, anomalies: List[Dict], workers: int) -> List[Dict]:
        ctx = TaskContext(self.run_id, self.run_id, "Explainer", timeout_seconds=300)

        def logic(anoms):
            if not anoms:
                return []
            results = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self.explainer.batch_explain, [a]): a for a in anoms
                }
                for future in concurrent.futures.as_completed(futures):
                    try:
                        res = future.result()
                        results.extend(res)
                    except Exception as e:
                        logger.error(f"Partial Explain Failure: {e}")

            out_file = self.run_dir / "enriched_anomalies.json"
            with open(out_file, "w") as f:
                json.dump(results, f, indent=2)
            self._add_artifact("explanations", str(out_file))
            return results

        return self._execute_task(logic, ctx, anomalies)

    @timeit_span("coordinator.act")
    def run_act(self, enriched: List[Dict]) -> List[Dict]:
        ctx = TaskContext(self.run_id, self.run_id, "Actor", timeout_seconds=120)

        def logic(items):
            if not items:
                return []
            res = self.actor.run_batch(items)
            self._add_artifact("actions_log", str(self.actor.action_log))
            return res

        return self._execute_task(logic, ctx, enriched)

    # --- Orchestration ---

    @timeit_span("coordinator.run_flow")
    def run(self, flow_config: Dict, inputs: Dict, session_id: str):
        logger.info(f"Starting Run {self.run_id}")

        manifest = {
            "run_id": self.run_id,
            "conversation_id": session_id,
            "start_ts": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "config": flow_config,
            "tasks": [],
            "artifacts": {},
        }

        try:
            csv_path = inputs.get("csv_path")
            workers = flow_config.get("parallelism", 3)

            snap = self.run_ingest(csv_path)
            anoms = self.run_detect(snap)
            enriched = self.run_explain(anoms[:5], workers)

            if flow_config.get("confirm_actions", True) and not self.dry_run:
                self.run_act(enriched)
            else:
                logger.info("Actions skipped (Dry Run or Confirmation False)")

            manifest["status"] = "completed"
            RUNS_TOTAL.labels(status="completed").inc()  # Metric

        except Exception as e:
            manifest["status"] = "failed"
            manifest["error"] = str(e)
            logger.error(f"Run Failed: {e}")
            RUNS_TOTAL.labels(status="failed").inc()  # Metric

        finally:
            manifest["end_ts"] = datetime.now(timezone.utc).isoformat()
            manifest["tasks"] = self.task_log
            manifest["artifacts"] = self.artifacts

            self._append_manifest_atomic(manifest)
            return manifest
