"""
observability/collector.py
Utilities to aggregate and parse observability logs.
"""

import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional


class LogCollector:
    def __init__(self, base_dir: str = "outputs/observability"):
        self.base_dir = Path(base_dir)

    def _load_jsonl(self, filename: str) -> List[Dict]:
        path = self.base_dir / filename
        data = []
        if path.exists():
            with open(path, "r") as f:
                for line in f:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return data

    def get_runs(self) -> pd.DataFrame:
        """Returns A2A runs as DataFrame."""
        data = self._load_jsonl("a2a_runs.jsonl")
        df = pd.DataFrame(data)
        if not df.empty:
            df["start_ts"] = pd.to_datetime(df["start_ts"])
            df["end_ts"] = pd.to_datetime(df["end_ts"])
            df["duration_sec"] = (df["end_ts"] - df["start_ts"]).dt.total_seconds()
        return df

    def get_traces(self, run_id: Optional[str] = None) -> pd.DataFrame:
        """Returns trace spans, optionally filtered by run/time."""
        data = self._load_jsonl("trace_spans.jsonl")
        df = pd.DataFrame(data)
        if not df.empty:
            df["start_ts"] = pd.to_datetime(df["start_ts"])
            df["end_ts"] = pd.to_datetime(df["end_ts"])
        return df

    def get_llm_calls(self) -> pd.DataFrame:
        """Returns LLM audit logs."""
        data = self._load_jsonl("llm_calls.jsonl")
        return pd.DataFrame(data)

    def get_actions(self) -> pd.DataFrame:
        """Returns Action audit logs (from ../actions)."""
        # Actions are stored in a sibling directory
        action_path = self.base_dir.parent / "actions" / "actions.jsonl"
        data = []
        if action_path.exists():
            with open(action_path, "r") as f:
                for line in f:
                    data.append(json.loads(line))
        return pd.DataFrame(data)
