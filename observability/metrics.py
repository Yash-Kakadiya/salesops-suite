"""
observability/metrics.py
Unified Metrics Collector (Prometheus + Local Snapshot).

- Uses dynamic OBSERVABILITY_DIR for snapshotting (good for tests/CI)
- Cleaner formatting + durable snapshot logic
- Exposes optional Prometheus HTTP endpoint
"""

import os
import json
import time
import threading
from pathlib import Path
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ---------------------------------------------------------------------
# METRIC DEFINITIONS
# ---------------------------------------------------------------------

RUNS_TOTAL = Counter(
    "salesops_runs_total", "Total number of Coordinator runs", ["status"]
)

LLM_CALLS = Counter(
    "salesops_llm_calls_total", "Total LLM API calls", ["model", "status"]
)

LLM_LATENCY = Histogram(
    "salesops_llm_latency_ms",
    "Latency of LLM calls",
    ["model"],
    buckets=(100, 500, 1000, 2000, 5000, 10000),
)

ACTIONS_TOTAL = Counter(
    "salesops_actions_total", "Total Actions executed", ["type", "status"]
)

MEMORY_OPS = Counter("salesops_memory_ops_total", "Total Memory Operations", ["op"])

# ---------------------------------------------------------------------
# SNAPSHOTTING
# ---------------------------------------------------------------------


def get_snapshot_file() -> Path:
    """
    Returns the snapshot file path.
    Uses OBSERVABILITY_DIR env var (better for testing / docker / CI).
    """
    out_dir = os.getenv("OBSERVABILITY_DIR", "outputs/observability")
    return Path(out_dir) / "metrics_snapshot.json"


def save_metrics_snapshot():
    """
    Dumps all 'salesops_*' Prometheus metrics to a JSON file.
    Used for Jupyter notebooks, dashboards, or debugging without /metrics.
    """
    from prometheus_client import REGISTRY

    data = []
    now = time.time()

    for metric in REGISTRY.collect():
        if metric.name.startswith("salesops"):
            for sample in metric.samples:
                data.append(
                    {
                        "name": sample.name,
                        "labels": sample.labels,
                        "value": sample.value,
                        "timestamp": now,
                    }
                )

    snapshot_file = get_snapshot_file()
    snapshot_file.parent.mkdir(parents=True, exist_ok=True)

    with open(snapshot_file, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------
# PROMETHEUS HTTP SERVER (OPTIONAL)
# ---------------------------------------------------------------------


def start_metrics_server(port=8001):
    """
    Starts a simple Prometheus /metrics server on the given port.
    Runs in a daemon thread so it does not block the main process.
    """
    from wsgiref.simple_server import make_server

    def app(environ, start_response):
        data = generate_latest()
        start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
        return [data]

    thread = threading.Thread(
        target=lambda: make_server("", port, app).serve_forever(), daemon=True
    )
    thread.start()
