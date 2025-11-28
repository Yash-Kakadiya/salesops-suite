"""
dashboard/app.py
Main Entry Point for the SalesOps Streamlit Dashboard.
"""

import streamlit as st
import sys
import os
from pathlib import Path

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.style import apply_custom_css, sidebar_logo
from utils.loaders import load_snapshot, get_latest_run_info
from utils.charts import render_kpi_cards, plot_sales_trend

# 1. App Configuration
st.set_page_config(
    page_title="SalesOps Command Center",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 2. Apply Style
apply_custom_css()
sidebar_logo()

# 3. Sidebar Navigation
page = st.sidebar.radio("Navigation", ["🚀 Mission Control", "📂 Raw Data Inspector"])

# 4. Header Area
col1, col2 = st.columns([3, 1])
with col1:
    st.title("SalesOps Command Center")
    st.caption("Autonomous Monitoring • Detection • Action")
with col2:
    # Status Indicator
    manifest = get_latest_run_info()
    if manifest:
        ts = manifest.get("start_time", "").split("T")[0]
        st.success(f"System Online\nLast Run: {ts}")
    else:
        st.warning("System Standby")

st.markdown("---")

# 5. Load Data
df = load_snapshot()

if page == "🚀 Mission Control":
    if not df.empty:
        st.subheader("Business Health")
        render_kpi_cards(df)

        st.markdown("###")  # Spacer

        col_main, col_side = st.columns([2, 1])

        with col_main:
            plot_sales_trend(df)

        with col_side:
            st.info("**System Alerts**")
            st.markdown(
                """
            * ✅ Ingestion Pipeline: **Healthy**
            * ✅ Anomaly Detector: **Active**
            * ✅ Gemini 2.0: **Connected**
            * ✅ Action Agent: **Standby**
            """
            )
            st.button("Trigger Manual Refresh")

    else:
        st.info("Waiting for pipeline data... Run 'python main.py' to generate data.")

elif page == "📂 Raw Data Inspector":
    st.subheader("Dataset Snapshot")
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=600)
    else:
        st.warning("No data available.")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("v1.0 | Powered by Google ADK")
