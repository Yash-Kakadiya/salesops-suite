import sys
import os
import json
import time
import pytest
import concurrent.futures
from unittest.mock import MagicMock, patch
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agents.a2a_coordinator import A2ACoordinator


@pytest.fixture
def coordinator(tmp_path):
    return A2ACoordinator(output_dir=str(tmp_path))


def test_sequential_flow_success(coordinator):
    """Verify end-to-end flow logic."""
    with patch.object(
        coordinator, "run_ingest", return_value="snap.parquet"
    ), patch.object(coordinator, "run_detect", return_value=[{"id": 1}]), patch.object(
        coordinator, "run_explain", return_value=[{"id": 1}]
    ), patch.object(
        coordinator, "run_act", return_value=[]
    ):

        manifest = coordinator.run({"confirm_actions": True}, {"csv_path": "t"}, "s1")
        assert manifest["status"] == "completed"
        # LOW PRIORITY FIX: Check artifact presence generally, not specific tasks array
        assert "run_id" in manifest


def test_stale_lock_removal(coordinator):
    """Test that stale lock files are cleaned up."""
    lock_file = coordinator.master_manifest_path.with_suffix(".lock")

    # Create a "stale" lock file (fake older time)
    with open(lock_file, "w") as f:
        f.write("stale")

    # Manually adjust mtime to 20 seconds ago
    old_time = time.time() - 20
    os.utime(lock_file, (old_time, old_time))

    # Attempt write - should remove stale lock and succeed
    coordinator._append_manifest_atomic({"status": "ok"})

    assert not lock_file.exists()
    assert coordinator.master_manifest_path.exists()


def test_timeout_enforcement(coordinator):
    """Verify that long-running tasks are killed."""

    def sleeping_beauty(*args):
        time.sleep(2)
        return "Woke up"

    from agents.a2a_coordinator import TaskContext

    ctx = TaskContext("run", "sess", "SleepyTask", timeout_seconds=0.1)

    with pytest.raises(Exception) as exc:
        coordinator._execute_task(sleeping_beauty, ctx)

    assert "Timed Out" in str(exc.value) or "exceeded" in str(exc.value)
