"""
observability/metrics.py
Metrics collector (Prometheus + Local Snapshot).
"""

import json
import time
import threading
from pathlib import Path
from typing import Dict
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# --- Metric Definitions ---

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

# --- Snapshotting ---

SNAPSHOT_FILE = Path("outputs/observability/metrics_snapshot.json")


def save_metrics_snapshot():
    """Dumps current metrics to JSON for Notebook visualization."""
    # This is a simplified dump of the internal Prometheus registry state
    # For a real scraper, use /metrics endpoint. For demo, this file is easier.

    from prometheus_client import REGISTRY

    data = []

    for metric in REGISTRY.collect():
        if metric.name.startswith("salesops"):
            for sample in metric.samples:
                data.append(
                    {
                        "name": sample.name,
                        "labels": sample.labels,
                        "value": sample.value,
                        "timestamp": time.time(),
                    }
                )

    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(data, f, indent=2)


# --- HTTP Server (Optional) ---
def start_metrics_server(port=8001):
    from wsgiref.simple_server import make_server

    def app(environ, start_response):
        data = generate_latest()
        start_response("200 OK", [("Content-Type", CONTENT_TYPE_LATEST)])
        return [data]

    t = threading.Thread(target=lambda: make_server("", port, app).serve_forever())
    t.daemon = True
    t.start()
