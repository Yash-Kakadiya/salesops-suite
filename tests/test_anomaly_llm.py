import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from agents.anomaly_llm_agent import AnomalyExplainerAgent


@pytest.fixture
def agent():
    return AnomalyExplainerAgent(dry_run=True)


def test_list_correction(agent):
    """Verify that string actions are converted to lists."""
    bad_schema = {
        "suggested_actions": "Just check the logs",  # String instead of List
        "explanation_short": "ok",
    }
    validated = agent._validate_response_schema(bad_schema)

    assert isinstance(validated["suggested_actions"], list)
    assert validated["suggested_actions"][0] == "Just check the logs"


def test_pii_redaction(agent):
    assert "REDACTED" in agent._redact_pii("CUST-12345678901")
    assert "REDACTED" in agent._redact_pii("user@example.com")


def test_circuit_breaker_skipped_flags(agent):
    """Verify robust skipping logic."""
    agent = AnomalyExplainerAgent(dry_run=False)
    agent.CIRCUIT_BREAKER_THRESHOLD = 1  # Trip immediately

    with patch.object(agent, "_call_llm_safe", side_effect=Exception("Fail")):
        batch = [{"id": 1}, {"id": 2}]
        results = agent.batch_explain(batch)

        # First failed
        assert "error" in results[0]
        # Second skipped
        assert results[1]["skipped"] is True
        assert results[1]["skipped_reason"] == "Circuit Breaker Tripped"


if __name__ == "__main__":
    # Quick sanity check
    a = AnomalyExplainerAgent(dry_run=True)
    print("Tests loaded.")
