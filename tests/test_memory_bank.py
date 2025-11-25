import sys
import os
import time
import pytest
import threading
import json
from unittest.mock import MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from memory.memory_bank import MemoryBank


@pytest.fixture
def bank(tmp_path):
    return MemoryBank(persistence_path=str(tmp_path / "bank.json"))


def test_upsert_and_query(bank):
    """Verify retrieval and ID return."""
    mid = bank.upsert("Critical failure in West region", {"type": "event"})
    assert mid

    # Mock vector for exact match logic if needed, or rely on simple local embedder
    results = bank.query("failure West", top_k=1)
    assert len(results) == 1
    assert results[0]["memory_id"] == mid
    assert results[0]["text"] == "Critical failure in West region"


def test_pii_redaction(bank):
    """Verify privacy patterns."""
    mid = bank.upsert("Email alice@corp.com now. Phone 555-0199.")
    res = bank.backend.get(mid)
    # Note: metadata["text"] holds the clean text
    stored_text = res["metadata"]["text"]
    assert "<EMAIL>" in stored_text
    assert "<PHONE>" in stored_text
    assert "alice@corp.com" not in stored_text


def test_ttl_expiration(bank):
    """Verify time-based eviction."""
    bank.upsert("Short memory", ttl_seconds=1)
    assert bank.backend.count() == 1
    time.sleep(2.0)  # Safe wait

    # Query triggers cleanup
    res = bank.query("Short")
    assert len(res) == 0
    assert bank.backend.count() == 0


def test_max_capacity_eviction(tmp_path):
    """Verify LRU eviction (Capacity)."""
    # Capacity 2
    b = MemoryBank(persistence_path=str(tmp_path / "cap.json"), max_memories=2)

    b.upsert("One")
    time.sleep(0.1)  # Ensure timestamp ordering
    b.upsert("Two")
    time.sleep(0.1)
    b.upsert("Three")  # Should evict "One"

    assert b.backend.count() == 2

    # Inspect store directly to verify "One" is gone
    found_texts = [v["metadata"]["text"] for v in b.backend.store.values()]
    assert "One" not in found_texts
    assert "Two" in found_texts
    assert "Three" in found_texts


def test_atomic_persistence(tmp_path):
    """Verify save/load."""
    p_path = tmp_path / "persist.json"
    b1 = MemoryBank(persistence_path=str(p_path))
    b1.upsert("Data")
    b1.save()

    assert p_path.exists()

    b2 = MemoryBank(persistence_path=str(p_path))
    assert b2.backend.count() == 1
    results = b2.query("Data")
    assert results[0]["text"] == "Data"


def test_concurrency(bank):
    """Verify thread safety."""

    def worker():
        for _ in range(10):
            bank.upsert(f"Thread data {threading.get_ident()}")
            bank.query("data")

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 5 threads * 10 upserts = 50
    assert bank.backend.count() == 50
