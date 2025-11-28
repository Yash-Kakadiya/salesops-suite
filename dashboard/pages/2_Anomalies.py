import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dashboard.utils.style import apply_custom_css, sidebar_logo
from dashboard.utils.loaders import load_anomalies
from dashboard.utils.charts import plot_anomaly_scatter

st.set_page_config(page_title="Anomaly Detection", page_icon="🚨", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("🚨 Detected Anomalies")
st.markdown(
    "Statistical outliers detected by **Z-Score** (Global Spikes) and **IQR** (Regional/Category Deviations)."
)

df = load_anomalies()

if not df.empty:
    # 1. Top Level Plot
    plot_anomaly_scatter(df)

    st.markdown("### 🔍 Anomaly Inspector")

    # 2. Filters
    c1, c2 = st.columns(2)
    min_score = c1.slider("Minimum Severity Score", 0.0, 100.0, 3.0)
    detector = c2.multiselect(
        "Detector Type", df["detector"].unique(), default=df["detector"].unique()
    )

    # Filter Logic
    filtered = df[
        (df["score"] >= min_score) & (df["detector"].isin(detector))
    ].sort_values("score", ascending=False)

    st.info(f"Showing {len(filtered)} anomalies based on filters.")

    # 3. Detailed Table
    st.dataframe(
        filtered[
            [
                "anomaly_id",
                "period_start",
                "entity_id",
                "metric",
                "value",
                "expected",
                "score",
                "reason",
            ]
        ],
        use_container_width=True,
        height=400,
    )
else:
    st.warning("No anomalies found in the latest run.")
