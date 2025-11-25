"""
memory/base.py
Abstract Base Classes for Memory Components.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

Vector = List[float]


class Embedder(ABC):
    """Interface for converting text to vector embeddings."""

    @abstractmethod
    def embed_text(self, text: str) -> Vector:
        """Returns a vector representation of the text."""
        pass

    @property
    @abstractmethod
    def vector_size(self) -> int:
        """Returns the dimensionality of vectors."""
        pass


class BaseMemoryBackend(ABC):
    """Interface for storage backends (InMemory, SQLite, etc)."""

    @abstractmethod
    def upsert(self, memory_id: str, embedding: Vector, metadata: Dict[str, Any]):
        """Inserts or updates a memory vector."""
        pass

    @abstractmethod
    def get(self, memory_id: str) -> Optional[Dict]:
        """
        Retrieves a single memory record.
        Returns: { 'memory_id': str, 'vector': List[float], 'metadata': Dict } or None.
        """
        pass

    @abstractmethod
    def query(
        self,
        vector: Vector,
        top_k: int = 5,
        filter_metadata: Dict = None,
        min_score: float = 0.0,
    ) -> List[Dict]:
        """
        Returns top_k most similar memories.
        Returns List of: { 'memory_id': str, 'text': str, 'metadata': Dict, '_score': float }
        """
        pass

    @abstractmethod
    def list(self, filter_metadata: Dict = None, limit: int = 100) -> List[Dict]:
        """
        Enumerates memories matching filters.
        Returns List of: { 'metadata': Dict }
        """
        pass

    @abstractmethod
    def delete(self, memory_id: str):
        """Removes a memory."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Returns total memory count."""
        pass
