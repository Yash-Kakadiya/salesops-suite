import streamlit as st
import pandas as pd
import plotly.express as px
import json
import sys
from pathlib import Path

# --- Add Project Root to Path BEFORE custom imports ---
# This tells Python to look in "salesops-suite/" so it can find "dashboard"
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))
# --------------------------------------------------------------------

# Now we can safely import from our local packages
from dashboard.utils.style import apply_custom_css, sidebar_logo
from dashboard.utils.charts import _style_fig
from observability.collector import LogCollector

st.set_page_config(page_title="Observability", page_icon="🔭", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("🔭 System Observability")
st.markdown("Deep dive into Traces, Latency, and LLM Performance.")

# Init Collector
# Resolve path relative to project root
OBS_DIR = project_root / "outputs" / "observability"
collector = LogCollector(str(OBS_DIR))

tab1, tab2 = st.tabs(["⏱️ Execution Traces", "🤖 AI Performance"])

with tab1:
    st.subheader("Pipeline Execution Waterfall")
    df_traces = collector.get_traces()

    if not df_traces.empty:
        # 1. Sort by start time
        df_traces = df_traces.sort_values("start_ts")

        # 2. Filter to recent runs (last 100)
        df_traces = df_traces.tail(100)

        # 3. Ensure visibility (min duration 1ms)
        df_traces["duration_ms"] = df_traces["duration_ms"].apply(lambda x: max(x, 1.0))

        fig = px.timeline(
            df_traces,
            x_start="start_ts",
            x_end="end_ts",
            y="name",
            color="component",
            hover_data=["duration_ms", "status", "error"],
            title="Recent Task Spans (Last 100 ops)",
            height=500,
        )

        # 4. Visual Styling
        fig.update_traces(marker_line_width=1, marker_line_color="white", opacity=0.9)
        fig.update_yaxes(autorange="reversed")

        # Apply High-Contrast Theme
        st.plotly_chart(_style_fig(fig), use_container_width=True)

        with st.expander("Raw Trace Data"):
            st.dataframe(df_traces)
    else:
        st.info("No traces found. Run the pipeline to generate telemetry.")

with tab2:
    st.subheader("LLM Audit Trail")
    df_llm = collector.get_llm_calls()

    if not df_llm.empty:
        # Metrics
        avg_lat = df_llm["latency_ms"].mean()
        total_tok = df_llm["est_tokens"].sum() if "est_tokens" in df_llm.columns else 0

        c1, c2 = st.columns(2)
        c1.metric("Avg Latency", f"{avg_lat:.0f} ms")
        c2.metric("Est. Tokens", f"{total_tok:,}")

        # Latency Hist
        fig = px.histogram(
            df_llm,
            x="latency_ms",
            nbins=20,
            title="Latency Distribution",
            color="model",
            marginal="box",
        )
        st.plotly_chart(_style_fig(fig), use_container_width=True)

        st.markdown("### Call History")
        cols_to_show = ["timestamp", "anomaly_id", "model", "latency_ms", "status"]
        # Filter cols that actually exist
        cols_to_show = [c for c in cols_to_show if c in df_llm.columns]

        st.dataframe(df_llm[cols_to_show].sort_values("timestamp", ascending=False))
    else:
        st.info("No LLM calls recorded.")
