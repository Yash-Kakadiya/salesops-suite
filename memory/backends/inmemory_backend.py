"""
memory/backends/inmemory_backend.py
Thread-safe Vector Store using Cosine Similarity.
"""

import math
import logging
import threading
from typing import List, Dict, Any, Tuple, Optional
from ..base import BaseMemoryBackend

logger = logging.getLogger(__name__)


class InMemoryBackend(BaseMemoryBackend):

    def __init__(self):
        self._lock = threading.Lock()
        self.store: Dict[str, Dict] = {}

    def upsert(self, memory_id: str, embedding: List[float], metadata: Dict[str, Any]):
        with self._lock:
            self.store[memory_id] = {
                "vector": embedding,
                "metadata": metadata,
                "memory_id": memory_id,
            }

    def get(self, memory_id: str) -> Optional[Dict]:
        with self._lock:
            # Return the FULL record (vector + metadata + id)
            return self.store.get(memory_id)

    def delete(self, memory_id: str):
        with self._lock:
            if memory_id in self.store:
                del self.store[memory_id]

    def count(self) -> int:
        with self._lock:
            return len(self.store)

    def list(self, filter_metadata: Dict = None, limit: int = 100) -> List[Dict]:
        results = []
        with self._lock:
            for data in self.store.values():
                meta = data["metadata"]
                if filter_metadata:
                    match = all(meta.get(k) == v for k, v in filter_metadata.items())
                    if not match:
                        continue
                results.append(meta)
                if len(results) >= limit:
                    break
        return results

    def _cosine_similarity(self, v1: List[float], v2: List[float]) -> float:
        dot_prod = sum(a * b for a, b in zip(v1, v2))
        norm_a = math.sqrt(sum(a * a for a in v1))
        norm_b = math.sqrt(sum(b * b for b in v2))

        if norm_a == 0 or norm_b == 0:
            # Debug log for zero vectors
            # logger.debug("Zero vector encountered in similarity check")
            return 0.0
        return dot_prod / (norm_a * norm_b)

    def query(
        self,
        query_vector: List[float],
        top_k: int = 5,
        filter_metadata: Dict = None,
        min_score: float = 0.0,
    ) -> List[Dict]:
        scores: List[Tuple[float, Dict]] = []

        with self._lock:
            for mem_id, data in self.store.items():
                # 1. Filter
                if filter_metadata:
                    match = all(
                        data["metadata"].get(k) == v for k, v in filter_metadata.items()
                    )
                    if not match:
                        continue

                # 2. Score
                score = self._cosine_similarity(query_vector, data["vector"])
                if score < min_score:
                    continue

                scores.append((score, data))

        # 3. Sort Descending
        scores.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, item in scores[:top_k]:
            # Structured Result Object
            res = {
                "memory_id": item["memory_id"],
                "text": item["metadata"].get("text", ""),
                "_score": round(score, 4),
                "metadata": item["metadata"],
            }
            results.append(res)

        return results
