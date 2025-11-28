import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Setup path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from dashboard.utils.style import apply_custom_css, sidebar_logo
from dashboard.utils.loaders import load_snapshot
from dashboard.utils.charts import (
    render_kpi_cards,
    plot_sales_trend,
    plot_segment_distribution,
    plot_regional_sales,
    plot_top_products,
)

st.set_page_config(page_title="KPI Dashboard", page_icon="📊", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("📊 Business Performance")

# 1. Load Data
df = load_snapshot()

if not df.empty:
    # 2. Filters
    with st.expander("🔎 Filter Data", expanded=False):
        c1, c2 = st.columns(2)
        regions = ["All"] + list(df["Region"].unique())
        cats = ["All"] + list(df["Category"].unique())

        sel_reg = c1.selectbox("Region", regions)
        sel_cat = c2.selectbox("Category", cats)

        # Apply
        if sel_reg != "All":
            df = df[df["Region"] == sel_reg]
        if sel_cat != "All":
            df = df[df["Category"] == sel_cat]

    # 3. Metrics
    render_kpi_cards(df)
    st.markdown("---")

    # 4. Charts Grid
    col1, col2 = st.columns(2)

    with col1:
        plot_sales_trend(df)
        plot_segment_distribution(df)

    with col2:
        plot_regional_sales(df)
        plot_top_products(df)

else:
    st.error("No data available. Run pipeline first.")
