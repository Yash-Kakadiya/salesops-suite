"""
dashboard/utils/loaders.py
Unified + Improved Data Access Layer for Streamlit.
- Robust path discovery
- Better error visibility
- Rich metadata extraction
"""

import json
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Dict, Any, List

# ---------------------------------------------------------
# ROBUST PATH FINDING
# ---------------------------------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
DATA_DIR = project_root / "dashboard_data"

# Fallback for cloud/docker/custom working directories
if not DATA_DIR.exists():
    DATA_DIR = Path("dashboard_data").resolve()


# ---------------------------------------------------------
# LOAD SNAPSHOT
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_snapshot() -> pd.DataFrame:
    """Loads the main sales dataset."""
    path = DATA_DIR / "snapshot.parquet"
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)

        # Normalize dates if present
        if "Order Date" in df.columns:
            df["Order Date"] = pd.to_datetime(df["Order Date"])

        return df

    except Exception as e:
        st.error(f"Failed to load snapshot: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# LOAD ANOMALIES
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_anomalies() -> pd.DataFrame:
    """Loads detected anomalies."""
    path = DATA_DIR / "anomalies.json"
    if not path.exists():
        return pd.DataFrame()

    try:
        with open(path, "r") as f:
            data = json.load(f)

        rows = data.get("all_anomalies", []) if isinstance(data, dict) else data
        return pd.DataFrame(rows)

    except Exception as e:
        st.error(f"Error loading anomalies: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# LOAD ENRICHED DATA
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_enriched() -> pd.DataFrame:
    """Loads AI-enriched explanations and flattens metadata."""
    path = DATA_DIR / "enriched.json"
    if not path.exists():
        return pd.DataFrame()

    try:
        with open(path, "r") as f:
            data = json.load(f)

        flattened = []

        for item in data:
            flat = item.copy()

            meta = item.get("meta", {})
            if isinstance(meta, dict):
                flat["model"] = meta.get("model", "Unknown")
                flat["latency"] = meta.get("latency_ms", 0)
                flat["version"] = meta.get("version", "1.0")  # From Version 1
            else:
                flat["model"] = "Unknown"
                flat["latency"] = 0
                flat["version"] = "1.0"

            flattened.append(flat)

        return pd.DataFrame(flattened)

    except Exception as e:
        st.error(f"Error loading enriched data: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# LOAD ACTIONS / AUDIT LOGS
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_actions() -> pd.DataFrame:
    """Loads action audit logs."""
    path = DATA_DIR / "actions.jsonl"
    rows = []

    try:
        if path.exists():
            with open(path, "r") as f:
                for line in f:
                    if line.strip():
                        rows.append(json.loads(line))

        df = pd.DataFrame(rows)

        if not df.empty and "result" in df.columns:
            df["status"] = df["result"].apply(
                lambda x: x.get("status") if isinstance(x, dict) else "unknown"
            )
            df["http_code"] = df["result"].apply(
                lambda x: x.get("http_code") if isinstance(x, dict) else 0
            )

        return df

    except Exception as e:
        st.error(f"Error loading actions: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# LOAD MANIFEST / PIPELINE RUN INFO
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def get_latest_run_info() -> Dict:
    """Loads the pipeline manifest.json."""
    path = DATA_DIR / "manifest.json"

    try:
        if path.exists():
            with open(path, "r") as f:
                return json.load(f)
        return {}

    except Exception as e:
        st.error(f"Error loading manifest: {e}")
        return {}


# ---------------------------------------------------------
# KPI HELPER (Added)
# ---------------------------------------------------------
def get_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculates high-level KPIs from the snapshot."""
    if df.empty:
        return {"revenue": 0, "profit": 0, "margin": 0}

    rev = df["Sales"].sum()
    profit = df["Profit"].sum()
    margin = (profit / rev) * 100 if rev > 0 else 0

    return {"revenue": rev, "profit": profit, "margin": margin}
