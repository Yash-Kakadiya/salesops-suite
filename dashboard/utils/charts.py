"""
dashboard/utils/charts.py
Reusable Plotly visualization components.
Fix: Simplified styling to rely on Streamlit's native Light/Dark theme adaptation.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


def _style_fig(fig):
    """
    Minimal styling that respects Streamlit's native Light/Dark theme.
    We remove hardcoded font colors so Streamlit can auto-adapt.
    """
    fig.update_layout(
        # transparent background allows the app theme to show through
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        # Standard margins
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
        # We do NOT set font color here; Streamlit does it automatically.
        # We set a neutral grid color that works on both White and Dark backgrounds.
        xaxis=dict(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)", zeroline=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)", zeroline=False),
    )
    return fig


def plot_sales_trend(df: pd.DataFrame):
    """Renders a daily sales trend line."""
    if df.empty:
        st.warning("No data for trend.")
        return

    daily = df.groupby("Order Date")["Sales"].sum().reset_index()

    fig = px.area(
        daily,
        x="Order Date",
        y="Sales",
        title="Revenue Trend",
    )

    fig.update_traces(
        line_color="#2D6CDF",
        fillcolor="rgba(45, 108, 223, 0.4)",  # Stronger fill (40%)
        line=dict(width=3),
    )

    st.plotly_chart(_style_fig(fig), use_container_width=True)


def plot_anomaly_scatter(df_anomalies: pd.DataFrame):
    """Renders anomalies by severity."""
    if df_anomalies.empty:
        st.info("No anomalies to display.")
        return

    if "period_start" in df_anomalies.columns:
        df_anomalies["date"] = pd.to_datetime(df_anomalies["period_start"])

    fig = px.scatter(
        df_anomalies,
        x="date",
        y="score",
        size="score",
        color="detector",
        hover_data=["entity_id", "metric", "value"],
        title="Anomaly Severity Radar",
        # Use accessible colors
        color_discrete_map={"zscore": "#E74C3C", "iqr": "#FFA500"},
        size_max=50,
    )

    # FIX: Dark border for visibility in Light Mode
    fig.update_traces(marker=dict(line=dict(width=1, color="#555555"), opacity=0.8))
    st.plotly_chart(_style_fig(fig), use_container_width=True)


def plot_segment_distribution(df: pd.DataFrame):
    """Pie chart of Sales by Segment."""
    if df.empty:
        return

    seg = df.groupby("Segment")["Sales"].sum().reset_index()

    fig = px.pie(
        seg,
        names="Segment",
        values="Sales",
        title="Sales by Segment",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig.update_traces(textinfo="percent+label")
    st.plotly_chart(_style_fig(fig), use_container_width=True)


def plot_regional_sales(df: pd.DataFrame):
    """Bar chart of Sales by Region."""
    if df.empty:
        return

    reg = (
        df.groupby("Region")["Sales"]
        .sum()
        .reset_index()
        .sort_values("Sales", ascending=True)
    )

    fig = px.bar(
        reg,
        x="Sales",
        y="Region",
        orientation="h",
        title="Sales by Region",
        text="Sales",  # Show values on bars
    )
    fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")
    st.plotly_chart(_style_fig(fig), use_container_width=True)


def plot_top_products(df: pd.DataFrame):
    """Bar chart of Top 10 Products."""
    if df.empty:
        return

    prod = (
        df.groupby("Product Name")["Sales"]
        .sum()
        .reset_index()
        .sort_values("Sales", ascending=True)
        .tail(10)
    )

    fig = px.bar(
        prod,
        x="Sales",
        y="Product Name",
        orientation="h",
        title="Top 10 Products",
        color_discrete_sequence=["#9B59B6"],
    )
    st.plotly_chart(_style_fig(fig), use_container_width=True)


def render_kpi_cards(df: pd.DataFrame):
    """Renders top-level metrics."""
    if df.empty:
        return

    rev = df["Sales"].sum()
    profit = df["Profit"].sum()
    margin = (profit / rev) * 100 if rev > 0 else 0
    orders = len(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 Revenue", f"${rev:,.0f}")
    c2.metric("📈 Profit", f"${profit:,.0f}")
    c3.metric("📊 Margin", f"{margin:.1f}%")
    c4.metric("📦 Orders", f"{orders:,}")
