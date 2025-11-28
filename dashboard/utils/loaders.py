"""
dashboard/utils/loaders.py
Data Access Layer for Streamlit.
Loads artifacts from the 'dashboard_data' directory.
"""

import json
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Dict, Any, List

# Path to the artifacts exported by run_pipeline.py
# dashboard/utils/loaders.py -> ../../dashboard_data
DATA_DIR = Path(__file__).resolve().parents[2] / "dashboard_data"


@st.cache_data(ttl=60)  # Cache for 1 minute so we see updates
def load_snapshot() -> pd.DataFrame:
    """Loads the main sales dataset."""
    path = DATA_DIR / "snapshot.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        if "Order Date" in df.columns:
            df["Order Date"] = pd.to_datetime(df["Order Date"])
        return df
    except Exception as e:
        st.error(f"Failed to load snapshot: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_anomalies() -> pd.DataFrame:
    """Loads detected anomalies."""
    path = DATA_DIR / "anomalies.json"
    if not path.exists():
        return pd.DataFrame()

    try:
        with open(path, "r") as f:
            data = json.load(f)
        # Handle different structures (list vs dict wrapper)
        rows = data.get("all_anomalies", []) if isinstance(data, dict) else data
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_enriched() -> pd.DataFrame:
    """Loads AI-enriched explanations."""
    path = DATA_DIR / "enriched.json"
    if not path.exists():
        return pd.DataFrame()

    try:
        with open(path, "r") as f:
            data = json.load(f)

        # FIX: Pre-process list to flatten 'meta' before DataFrame creation
        flattened_data = []
        for item in data:
            # Copy basic fields
            flat_item = item.copy()

            # Extract Meta fields safely
            meta = item.get("meta", {})
            if isinstance(meta, dict):
                flat_item["model"] = meta.get("model", "Unknown")
                flat_item["latency"] = meta.get("latency_ms", 0)
                flat_item["version"] = meta.get("version", "1.0")
            else:
                flat_item["model"] = "Unknown"
                flat_item["latency"] = 0

            flattened_data.append(flat_item)

        return pd.DataFrame(flattened_data)
    except Exception as e:
        print(f"Error loading enriched: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_actions() -> pd.DataFrame:
    """Loads action audit logs."""
    path = DATA_DIR / "actions.jsonl"
    data = []
    if path.exists():
        with open(path, "r") as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))

    df = pd.DataFrame(data)

    if not df.empty:
        # FIX: Extract nested fields
        if "result" in df.columns:
            df["status"] = df["result"].apply(
                lambda x: x.get("status") if isinstance(x, dict) else "unknown"
            )
            df["http_code"] = df["result"].apply(
                lambda x: x.get("http_code") if isinstance(x, dict) else 0
            )

    return df


@st.cache_data(ttl=60)
def get_latest_run_info() -> Dict:
    """Loads the pipeline manifest."""
    path = DATA_DIR / "manifest.json"
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {}
