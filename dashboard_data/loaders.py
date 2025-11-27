"""
dashboard_data/loaders.py
Data Access Layer for the Streamlit Dashboard.
Handles loading, parsing, and formatting of pipeline artifacts.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

# Define Base Path (Relative to this file)
DATA_DIR = Path(__file__).parent


def _load_json(filename: str) -> List[Dict]:
    path = DATA_DIR / filename
    if not path.exists():
        return []
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Error loading {filename}: {e}")
        return []


def _load_jsonl(filename: str) -> List[Dict]:
    path = DATA_DIR / filename
    data = []
    if path.exists():
        try:
            with open(path, "r") as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
        except Exception as e:
            print(f"⚠️ Error loading {filename}: {e}")
    return data


def _load_parquet(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"⚠️ Error loading {filename}: {e}")
        return pd.DataFrame()


# --- Public Loaders ---


def load_snapshot() -> pd.DataFrame:
    """Loads the cleaned sales data snapshot."""
    df = _load_parquet("snapshot.parquet")
    if not df.empty and "Order Date" in df.columns:
        df["Order Date"] = pd.to_datetime(df["Order Date"])
    return df


def load_anomalies() -> pd.DataFrame:
    """Loads statistical anomalies."""
    data = _load_json("anomalies.json")
    # The JSON structure is {"all_anomalies": [...]}
    if isinstance(data, dict):
        raw_list = data.get("all_anomalies", [])
    else:
        raw_list = data  # Fallback if structure changes

    df = pd.DataFrame(raw_list)
    if not df.empty:
        # Normalize dates
        if "period_start" in df.columns:
            df["period_start"] = pd.to_datetime(df["period_start"])
    return df


def load_enriched() -> pd.DataFrame:
    """Loads AI-enriched anomalies (Explanations)."""
    raw_list = _load_json("enriched.json")
    df = pd.DataFrame(raw_list)

    if not df.empty:
        # Flatten Metadata if needed
        if "meta" in df.columns:
            df["model"] = df["meta"].apply(lambda x: x.get("model") if x else None)
            df["latency"] = df["meta"].apply(
                lambda x: x.get("latency_ms") if x else None
            )
    return df


def load_actions() -> pd.DataFrame:
    """Loads the action execution log."""
    raw_list = _load_jsonl("actions.jsonl")
    df = pd.DataFrame(raw_list)

    if not df.empty:
        # Convert timestamps
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Flatten Result
        if "result" in df.columns:
            df["status"] = df["result"].apply(
                lambda x: x.get("status") if isinstance(x, dict) else "unknown"
            )
            df["http_code"] = df["result"].apply(
                lambda x: x.get("http_code") if isinstance(x, dict) else 0
            )

    return df


def load_manifest() -> Dict[str, Any]:
    """Loads the latest run manifest."""
    # Try json first, then jsonl (depending on export format)
    # Our pipeline exports 'manifest.json'
    data = _load_json("manifest.json")
    if isinstance(data, dict):
        return data
    return {}


def get_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculates high-level KPIs from the snapshot."""
    if df.empty:
        return {"revenue": 0, "profit": 0, "margin": 0}

    rev = df["Sales"].sum()
    profit = df["Profit"].sum()
    margin = (profit / rev) * 100 if rev > 0 else 0

    return {"revenue": rev, "profit": profit, "margin": margin}
