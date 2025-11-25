import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agents.a2a_coordinator import A2ACoordinator


@pytest.fixture
def coordinator(tmp_path):
    # Redirect observability via Env Var
    os.environ["OBSERVABILITY_DIR"] = str(tmp_path / "observability")
    return A2ACoordinator(output_dir=str(tmp_path), dry_run=True)


def test_full_run_emits_telemetry(coordinator):
    """Simulate run and check traces."""

    with patch.object(coordinator, "run_ingest", return_value="snap"), patch.object(
        coordinator, "run_detect", return_value=[{"id": "1"}]
    ), patch.object(
        coordinator, "run_explain", return_value=[{"id": "1"}]
    ), patch.object(
        coordinator, "run_act", return_value=[]
    ):

        coordinator.run({}, {"csv_path": "test.csv"}, "sess_1")

    # Check Trace Spans (Location determined by Env Var)
    trace_path = coordinator.observability_dir / "trace_spans.jsonl"

    assert trace_path.exists()
    with open(trace_path, "r") as f:
        lines = f.readlines()

    span_names = [json.loads(l)["name"] for l in lines]
    assert "coordinator.run_flow" in span_names
