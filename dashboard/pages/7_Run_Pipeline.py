import streamlit as st
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dashboard.utils.style import apply_custom_css, sidebar_logo

# Import the actual driver function
from scripts.run_pipeline import run_integration_demo

st.set_page_config(page_title="Run Pipeline", page_icon="▶️", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("▶️ Trigger Autonomous Pipeline")
st.markdown("Manually trigger an end-to-end run of the SalesOps Agents.")

# 1. Config
with st.container():
    st.subheader("Configuration")
    c1, c2 = st.columns(2)

    use_dry_run = c1.checkbox(
        "Dry Run Mode", value=False, help="If checked, no emails/tickets will be sent."
    )
    workers = c2.slider("Parallel AI Workers", 1, 5, 3)

    data_path = "data/raw/superstore.csv"  # Fixed for demo simplicity

# 2. Execution
if st.button("🚀 Start Pipeline", type="primary"):
    status_area = st.empty()
    logs_area = st.empty()

    status_area.info("⏳ Pipeline Initializing...")

    # Progress Bar
    progress = st.progress(0)

    try:
        # We wrap this in a spinner
        with st.spinner("Agents working... (Check terminal for live logs)"):
            # Update status
            progress.progress(10)
            time.sleep(0.5)

            # Run the Script Logic
            # Note: This runs synchronously. For a real app, we'd use a background thread/queue.
            # For this demo, sync is fine and ensures we see the result immediately.
            success = run_integration_demo(
                data_path=data_path,
                output_dir="outputs/demo_run",
                workers=workers,
                dry_run=use_dry_run,
            )

            progress.progress(100)

        if success:
            status_area.success("✅ Pipeline Completed Successfully!")
            st.balloons()
            st.markdown("### Next Steps")
            st.info(
                "Navigate to **Mission Control** or **Anomalies** pages to see the new data."
            )

            # Button to reload
            if st.button("Refresh Dashboard Data"):
                st.cache_data.clear()
                st.experimental_rerun()
        else:
            status_area.error("❌ Pipeline Failed. Check console logs.")

    except Exception as e:
        status_area.error(f"Critical Error: {e}")
