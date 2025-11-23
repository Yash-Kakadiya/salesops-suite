import sys
import os
import pytest
import json
from unittest.mock import MagicMock, patch

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from agents.action_agent import ActionAgent


@pytest.fixture
def agent(tmp_path):
    return ActionAgent(output_dir=str(tmp_path))


def test_idempotency_generation(agent):
    """Same inputs must generate same key."""
    key1 = agent._generate_idempotency_key("anom_1", "create_ticket")
    key2 = agent._generate_idempotency_key("anom_1", "create_ticket")
    assert key1 == key2


@patch("requests.post")
def test_client_error_no_retry(mock_post, agent):
    """422 Validation Error (from Server) should NOT retry."""
    # Mock API response 422 Unprocessable Entity
    mock_post.return_value.status_code = 422
    mock_post.return_value.text = "Missing Field"

    # FIX: Use a VALID payload so it passes local schema validation
    valid_payload = {"title": "Test Ticket", "priority": "High", "anomaly_id": "a"}

    plan = {
        "action_id": "1",
        "anomaly_id": "a",
        "type": "create_ticket",
        "payload": valid_payload,
        "idempotency_key": "k",
    }

    res = agent.execute_action(plan)

    assert res["status"] == "client_error"
    assert mock_post.call_count == 1  # Called once, failed, no retry


@patch("requests.post")
def test_rate_limit_retry(mock_post, agent):
    """429 should retry after wait."""
    agent.RETRY_BACKOFF = 0.01  # Speed up test

    # FIX: Use a VALID payload so it passes local schema validation
    valid_payload = {"title": "Test Ticket", "priority": "High", "anomaly_id": "a"}

    # Sequence: 1. 429 (Wait) -> 2. 201 (Success)
    resp_429 = MagicMock(status_code=429)
    resp_429.headers = {"Retry-After": "0"}

    resp_201 = MagicMock(status_code=201, json=lambda: {"id": "ok"})

    mock_post.side_effect = [resp_429, resp_201]

    plan = {
        "action_id": "1",
        "anomaly_id": "a",
        "type": "create_ticket",
        "payload": valid_payload,
        "idempotency_key": "k",
    }

    res = agent.execute_action(plan)

    assert res["status"] == "success"
    assert res["attempts"] == 2


if __name__ == "__main__":
    print("Run with pytest")
