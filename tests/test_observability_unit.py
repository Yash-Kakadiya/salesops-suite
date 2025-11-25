import sys
import os
import json
import pytest
import logging
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from observability.logger import get_logger, timeit_span, _current_trace_id
from observability.metrics import RUNS_TOTAL


@pytest.fixture
def obs_dir(tmp_path):
    """Sets ENV var to redirect logs/traces to temp dir."""
    old_env = os.environ.get("OBSERVABILITY_DIR")
    os.environ["OBSERVABILITY_DIR"] = str(tmp_path)
    yield tmp_path
    # Cleanup
    if old_env:
        os.environ["OBSERVABILITY_DIR"] = old_env
    else:
        del os.environ["OBSERVABILITY_DIR"]


def test_logger_structure(obs_dir):
    """Verify logs are valid JSON with trace context."""
    log_file = obs_dir / "test_component.jsonl"

    # Pass output_dir explicitly to ensure it uses the fixture path
    logger = get_logger("TestComponent", output_dir=str(obs_dir))

    token = _current_trace_id.set("test-trace-123")
    logger.info("Test message")
    _current_trace_id.reset(token)

    # Flush handlers to ensure write
    for h in logger.handlers:
        h.flush()

    assert log_file.exists()
    with open(log_file, "r") as f:
        line = f.readline()
        entry = json.loads(line)

    assert entry["component"] == "TestComponent"
    assert entry["message"] == "Test message"
    assert entry["trace_id"] == "test-trace-123"


def test_trace_parenting(obs_dir):
    """Verify nested spans."""

    @timeit_span("child_op")
    def child():
        pass

    @timeit_span("parent_op")
    def parent():
        child()

    parent()

    trace_file = obs_dir / "trace_spans.jsonl"
    assert trace_file.exists()

    spans = []
    with open(trace_file, "r") as f:
        for line in f:
            spans.append(json.loads(line))

    assert len(spans) == 2
    child_span = next(s for s in spans if s["name"] == "child_op")
    parent_span = next(s for s in spans if s["name"] == "parent_op")

    assert child_span["parent_span_id"] == parent_span["span_id"]


def test_metrics_increment():
    """Verify Prometheus counters."""
    before = RUNS_TOTAL.labels(status="completed")._value.get()
    RUNS_TOTAL.labels(status="completed").inc()
    after = RUNS_TOTAL.labels(status="completed")._value.get()
    assert after == before + 1
