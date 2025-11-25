"""
agents/memory_agent.py
Bridge Agent for interacting with the Memory Bank.
Translates Business Objects into Semantic Memories.
"""

import logging
from typing import Dict, Any
from memory.memory_bank import MemoryBank

logger = logging.getLogger(__name__)


class MemoryAgent:
    """
    Agent Interface for Long-Term Memory.
    """

    def __init__(self):
        # Connect to the Production Bank
        self.bank = MemoryBank(persistence_path="../outputs/memory/memory_bank.json")

    def remember_anomaly_resolution(self, anomaly: Dict, action: Dict):
        """
        Learns from a completed cycle.
        Stores: "On [Date], [Metric] for [Entity] had anomaly. Explanation... Action..."
        """
        # Construct semantic string for future retrieval
        text = (
            f"Anomaly in {anomaly.get('entity_id')} ({anomaly.get('metric')}). "
            f"Severity: {anomaly.get('score')}. "
            f"Explanation: {anomaly.get('explanation_short')}. "
            f"Action Taken: {action.get('type')}."
        )

        metadata = {
            "type": "resolution",
            "entity": anomaly.get("entity_id"),
            "metric": anomaly.get("metric"),
            "score": anomaly.get("score"),
            "action_type": action.get("type"),
        }

        mid = self.bank.upsert(text, metadata)
        self.bank.save()  # Persist immediately
        logger.info(f"🧠 Learned new experience: {mid}")

    def retrieve_relevant_history(self, anomaly: Dict, top_k=3) -> str:
        """
        Finds past anomalies similar to the current one.
        Returns a formatted context string for the LLM.
        """
        # Query based on the problem description
        query = f"Anomaly {anomaly.get('entity_id')} {anomaly.get('metric')} {anomaly.get('level')}"

        results = self.bank.query(query, top_k=top_k, min_score=0.2)

        if not results:
            return "No relevant past events found."

        # Format for LLM Context
        context_lines = ["**Relevant Past Events (Learned History):**"]
        for res in results:
            score = res.get("_score", 0)
            text = res.get("text", "")
            # Include relative time if available
            date = res.get("metadata", {}).get("created_at", "").split("T")[0]
            context_lines.append(f"- [{date}] (Sim: {score:.2f}) {text}")

        return "\n".join(context_lines)
