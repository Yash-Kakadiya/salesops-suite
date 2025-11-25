"""
memory/memory_bank.py
Enterprise Memory Controller with Observability.
"""

import time
import uuid
import json
import re
import logging
import os
import tempfile
import threading
import datetime as dt
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from .base import BaseMemoryBackend, Embedder
from .embedder_local import LocalEmbedder
from .backends.inmemory_backend import InMemoryBackend

# Observability
from observability.logger import timeit_span, get_logger
from observability.metrics import MEMORY_OPS

logger = get_logger("MemoryBank")


class MemoryBank:

    SCHEMA_VERSION = "1.1"

    def __init__(
        self,
        persistence_path: str = "../outputs/memory/memory_bank.json",
        store_pii: bool = False,
        max_memories: int = 1000,
    ):
        self.embedder: Embedder = LocalEmbedder()
        self.backend: BaseMemoryBackend = InMemoryBackend()

        self.persistence_path = Path(persistence_path).resolve()
        self.persistence_path.parent.mkdir(parents=True, exist_ok=True)

        self.store_pii = store_pii
        self.max_memories = max_memories

        self.audit_file = (
            self.persistence_path.parent.parent / "observability" / "memory_runs.jsonl"
        )
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)

        self.stats = {"upserts": 0, "queries": 0, "evictions": 0, "errors": 0}
        self._stats_lock = threading.Lock()

        self.load()

    def metrics(self) -> Dict[str, int]:
        with self._stats_lock:
            return self.stats.copy()

    def _audit(self, op: str, details: Dict):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "op": op,
            **details,
        }
        try:
            with open(self.audit_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    def _redact_pii(self, text: str) -> str:
        if not text or self.store_pii:
            return text
        text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "<EMAIL>", text)
        text = re.sub(r"\b(\d{3}[-.]?)?\d{3}[-.]?\d{4}\b", "<PHONE>", text)
        text = re.sub(r"\b(?:\d{4}[- ]?){3}\d{4}\b", "<CREDIT_CARD>", text)
        text = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "<SSN>", text)
        return text

    def _parse_iso(self, date_str: str) -> Optional[datetime]:
        try:
            if date_str.endswith("Z"):
                date_str = date_str[:-1] + "+00:00"
            return datetime.fromisoformat(date_str)
        except ValueError:
            return None

    def cleanup_expired(self):
        """Removes expired memories."""
        now = datetime.now(timezone.utc)
        expired_ids = []

        if hasattr(self.backend, "store") and isinstance(self.backend.store, dict):
            lock = getattr(self.backend, "_lock", threading.Lock())
            with lock:
                keys = list(self.backend.store.keys())
                for k in keys:
                    item = self.backend.store[k]
                    meta = item.get("metadata", {})
                    expires_str = meta.get("expires_at")
                    if expires_str:
                        exp_dt = self._parse_iso(expires_str)
                        if exp_dt and exp_dt < now:
                            del self.backend.store[k]
                            expired_ids.append(k)

        if expired_ids:
            logger.info(f"Expired {len(expired_ids)} memories.")
            with self._stats_lock:
                self.stats["evictions"] += len(expired_ids)
            self._audit("expire", {"count": len(expired_ids)})

        current_count = self.backend.count()
        if current_count > self.max_memories:
            to_remove = current_count - self.max_memories
            if hasattr(self.backend, "store"):
                lock = getattr(self.backend, "_lock", threading.Lock())
                with lock:
                    sorted_keys = sorted(
                        self.backend.store.keys(),
                        key=lambda k: self.backend.store[k]["metadata"].get(
                            "created_at", ""
                        ),
                    )
                    for k in sorted_keys[:to_remove]:
                        del self.backend.store[k]
            with self._stats_lock:
                self.stats["evictions"] += to_remove

    @timeit_span("memory.upsert")
    def upsert(
        self,
        text: str,
        metadata: Dict[str, Any] = None,
        ttl_seconds: int = None,
        memory_id: str = None,
    ) -> str:
        if not text:
            raise ValueError("Text cannot be empty")
        MEMORY_OPS.labels(op="upsert").inc()

        self.cleanup_expired()

        t0 = time.time()
        clean_text = self._redact_pii(text)
        mid = memory_id or str(uuid.uuid4())

        meta = metadata.copy() if metadata else {}
        meta["text"] = clean_text
        meta["created_at"] = datetime.now(timezone.utc).isoformat()

        if ttl_seconds:
            import datetime as dt

            future = datetime.now(timezone.utc) + dt.timedelta(seconds=ttl_seconds)
            meta["expires_at"] = future.isoformat()

        try:
            vector = self.embedder.embed_text(clean_text)
            self.backend.upsert(mid, vector, meta)
            self.cleanup_expired()

            latency = (time.time() - t0) * 1000
            with self._stats_lock:
                self.stats["upserts"] += 1
            self._audit("upsert", {"memory_id": mid, "latency_ms": latency})
            return mid
        except Exception as e:
            with self._stats_lock:
                self.stats["errors"] += 1
            logger.error(f"Upsert failed: {e}")
            raise e

    @timeit_span("memory.query")
    def query(
        self,
        query_text: str,
        top_k: int = 3,
        filter_metadata: Dict = None,
        min_score: float = 0.0,
    ) -> List[Dict]:
        MEMORY_OPS.labels(op="query").inc()
        t0 = time.time()

        try:
            vector = self.embedder.embed_text(query_text)
            results = self.backend.query(
                vector,
                top_k=top_k,
                filter_metadata=filter_metadata,
                min_score=min_score,
            )

            now = datetime.now(timezone.utc)
            valid_results = []
            for r in results:
                exp_str = r["metadata"].get("expires_at")
                if exp_str:
                    exp_dt = self._parse_iso(exp_str)
                    if exp_dt and exp_dt < now:
                        continue
                valid_results.append(r)

            self.cleanup_expired()

            latency = (time.time() - t0) * 1000
            with self._stats_lock:
                self.stats["queries"] += 1
            returned_ids = [r["memory_id"] for r in valid_results]
            self._audit(
                "query",
                {
                    "query_len": len(query_text),
                    "result_count": len(valid_results),
                    "ids": returned_ids,
                    "latency_ms": latency,
                },
            )
            return valid_results
        except Exception as e:
            with self._stats_lock:
                self.stats["errors"] += 1
            logger.error(f"Query failed: {e}")
            return []

    def save(self):
        # (Code same as Day 8 - no new instrumentation needed for save)
        if hasattr(self.backend, "store"):
            lock = getattr(self.backend, "_lock", threading.Lock())
            with lock:
                payload = {
                    "__schema_version": self.SCHEMA_VERSION,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "store": self.backend.store,
                }
                try:
                    with tempfile.NamedTemporaryFile(
                        "w", dir=self.persistence_path.parent, delete=False
                    ) as tmp:
                        json.dump(payload, tmp, indent=2, default=str)
                        tmp_name = tmp.name
                    os.replace(tmp_name, self.persistence_path)
                except Exception:
                    if "tmp_name" in locals() and os.path.exists(tmp_name):
                        os.remove(tmp_name)

    def load(self):
        # (Code same as Day 8)
        if self.persistence_path.exists():
            try:
                with open(self.persistence_path, "r") as f:
                    data = json.load(f)
                if hasattr(self.backend, "store"):
                    lock = getattr(self.backend, "_lock", threading.Lock())
                    with lock:
                        self.backend.store = data.get("store", {})
            except Exception:
                pass
