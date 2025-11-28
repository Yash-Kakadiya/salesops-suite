import streamlit as st
import pandas as pd
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dashboard.utils.style import apply_custom_css, sidebar_logo
from dashboard.utils.loaders import load_actions

st.set_page_config(page_title="Action Audit", page_icon="⚡", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("⚡ Action Execution Audit")
st.markdown("Traceability of every side-effect (API Call) executed by the agent.")

df = load_actions()

if not df.empty:
    # 1. KPI Summary
    total = len(df)
    success = len(df[df["status"] == "success"])
    failed = len(df[df["status"] != "success"])
    success_rate = (success / total * 100) if total > 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Actions", total)
    c2.metric("Success Rate", f"{success_rate:.1f}%")
    c3.metric("Failures/Retries", failed, delta_color="inverse")

    st.markdown("---")

    # 2. Filter
    status_filter = st.multiselect(
        "Filter Status", df["status"].unique(), default=df["status"].unique()
    )
    filtered = df[df["status"].isin(status_filter)].sort_values(
        "timestamp", ascending=False
    )

    # 3. Detailed Log
    for idx, row in filtered.iterrows():
        with st.expander(
            f"{row['timestamp']} | {row['type']} | {row['status'].upper()}",
            expanded=False,
        ):
            c_a, c_b = st.columns(2)

            with c_a:
                st.markdown("**Action Metadata**")
                st.write(f"**Action ID:** `{row['action_id']}`")
                st.write(f"**Anomaly ID:** `{row['anomaly_id']}`")
                st.write(f"**Idempotency Key:** `{row.get('idempotency_key', 'N/A')}`")

            with c_b:
                st.markdown("**Result Output**")
                # Formatting JSON output
                if "result" in row and isinstance(row["result"], dict):
                    st.json(row["result"])
                else:
                    st.text(str(row.get("result", "")))

else:
    st.info("No actions recorded in the latest run.")
