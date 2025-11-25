"""
memory/embedder_local.py
Lightweight Embedder for Demo Purposes.
Uses TF-IDF if available, else deterministic hashing.
"""

import math
import hashlib
import logging
from typing import List
from .base import Embedder

logger = logging.getLogger(__name__)

try:
    from sklearn.feature_extraction.text import TfidfVectorizer

    # Ensure determinism for TF-IDF if re-trained
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


class LocalEmbedder(Embedder):

    def __init__(self, vector_size=384):
        self._vector_size = vector_size
        self.use_sklearn = HAS_SKLEARN

        if self.use_sklearn:
            try:
                self.vectorizer = TfidfVectorizer(
                    max_features=vector_size, stop_words="english"
                )
                # Seed vocabulary for deterministic behavior
                self.vectorizer.fit(
                    [
                        "sales revenue profit loss quarter growth",
                        "anomaly detection spike drop increase decrease",
                        "customer region product category technology furniture",
                        "action ticket email notification escalation",
                    ]
                )
            except Exception as e:
                logger.warning(f"Sklearn init failed: {e}. Falling back to hashing.")
                self.use_sklearn = False

    @property
    def vector_size(self) -> int:
        return self._vector_size

    def _normalize(self, vec: List[float]) -> List[float]:
        """Ensure unit norm for cosine similarity stability."""
        norm = math.sqrt(sum(x * x for x in vec))
        if norm == 0:
            return vec
        return [x / norm for x in vec]

    def _hash_embedding(self, text: str) -> List[float]:
        """Fallback: Deterministic random projection."""
        seed = int(hashlib.md5(text.encode()).hexdigest(), 16)
        import random

        r = random.Random(seed)
        vec = [r.uniform(-1, 1) for _ in range(self._vector_size)]
        return self._normalize(vec)

    def embed_text(self, text: str) -> List[float]:
        if not text:
            return [0.0] * self._vector_size

        try:
            if self.use_sklearn:
                # Convert to dense list
                vec = self.vectorizer.transform([text]).toarray()[0].tolist()
                if len(vec) < self._vector_size:
                    vec += [0.0] * (self._vector_size - len(vec))
                return self._normalize(vec)
        except Exception as e:
            logger.warning(f"Embedding failed: {e}. Fallback to hash.")

        return self._hash_embedding(text)
