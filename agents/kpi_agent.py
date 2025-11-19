"""
agents/kpi_agent.py

KPI Agent for SalesOps Suite.

Responsibilities:
- Compute core KPIs: total revenue, total profit, avg order value, revenue by period, rolling metrics
- Compute category/region breakdowns
- Compute profit margin and loss statistics
- Provide small helper functions that accept a DataFrame (pre-cleaned)
- Return JSON-serializable structures suitable for dashboards or downstream agents

Usage:
    from agents.kpi_agent import KPIAgent
    k = KPIAgent(df)
    k.total_revenue()
    k.revenue_by_period(freq="W")
    k.top_categories(n=10)
"""

from typing import Dict, Any
import pandas as pd
import numpy as np


class KPIAgent:
    def __init__(
        self,
        df: pd.DataFrame,
        date_col: str = "Order Date",
        revenue_col: str = "Sales",
        profit_col: str = "Profit",
        order_id_col: str = "Order ID",
    ):
        """
        df: pre-cleaned DataFrame (dates converted)
        """
        self.df = df.copy()
        self.date_col = date_col
        self.revenue_col = revenue_col
        self.profit_col = profit_col
        self.order_id_col = order_id_col

        # Basic sanity
        if (
            self.date_col in self.df.columns
            and not pd.api.types.is_datetime64_any_dtype(self.df[self.date_col])
        ):
            self.df[self.date_col] = pd.to_datetime(
                self.df[self.date_col], errors="coerce"
            )

    # ---------- Core KPIs ----------
    def total_revenue(self) -> float:
        return float(self.df[self.revenue_col].sum())

    def total_profit(self) -> float:
        return float(self.df[self.profit_col].sum())

    def orders_count(self) -> int:
        if self.order_id_col in self.df.columns:
            return int(self.df[self.order_id_col].nunique())
        return int(len(self.df))

    def avg_order_value(self) -> float:
        orders = self.orders_count()
        if orders == 0:
            return 0.0
        return float(self.total_revenue() / max(1, orders))

    def profit_margin(self) -> float:
        rev = self.total_revenue()
        if rev == 0:
            return 0.0
        return float(self.total_profit() / rev)

    # ---------- Time-series KPIs ----------
    def revenue_by_period(self, freq: str = "D") -> pd.Series:
        """
        freq: pandas offset alias, e.g., 'D', 'W', 'M'
        returns: Series indexed by period end timestamp
        """
        if self.date_col not in self.df.columns:
            raise KeyError(f"{self.date_col} missing from DataFrame")
        ts = (
            self.df.set_index(self.date_col)
            .resample(freq)[self.revenue_col]
            .sum()
            .fillna(0)
        )
        return ts

    def profit_by_period(self, freq: str = "D") -> pd.Series:
        if self.date_col not in self.df.columns:
            raise KeyError(f"{self.date_col} missing from DataFrame")
        ts = (
            self.df.set_index(self.date_col)
            .resample(freq)[self.profit_col]
            .sum()
            .fillna(0)
        )
        return ts

    def rolling_metric(
        self, series: pd.Series, window: int = 7, min_periods: int = 1
    ) -> pd.Series:
        return series.rolling(window=window, min_periods=min_periods).mean()

    # ---------- Breakdown KPIs ----------
    def revenue_by_category(self, n: int = 10) -> pd.DataFrame:
        if "Category" in self.df.columns:
            out = (
                self.df.groupby("Category")[self.revenue_col]
                .sum()
                .sort_values(ascending=False)
                .head(n)
                .reset_index()
            )
            out.columns = ["Category", "Revenue"]
            return out
        return pd.DataFrame(columns=["Category", "Revenue"])

    def revenue_by_region(self, n: int = 10) -> pd.DataFrame:
        if "Region" in self.df.columns:
            out = (
                self.df.groupby("Region")[self.revenue_col]
                .sum()
                .sort_values(ascending=False)
                .head(n)
                .reset_index()
            )
            out.columns = ["Region", "Revenue"]
            return out
        return pd.DataFrame(columns=["Region", "Revenue"])

    def profit_by_category(self, n: int = 10) -> pd.DataFrame:
        if "Category" in self.df.columns:
            out = (
                self.df.groupby("Category")[self.profit_col]
                .sum()
                .sort_values(ascending=False)
                .head(n)
                .reset_index()
            )
            out.columns = ["Category", "Profit"]
            return out
        return pd.DataFrame(columns=["Category", "Profit"])

    # ---------- Anomaly-friendly helpers ----------
    def negative_profit_orders(self, n: int = 20) -> pd.DataFrame:
        if self.profit_col not in self.df.columns:
            return pd.DataFrame()
        out = (
            self.df[self.df[self.profit_col] < 0]
            .sort_values(by=self.profit_col)
            .head(n)
        )
        return out

    def revenue_zscore(self, freq: str = "D") -> pd.DataFrame:
        """
        Produce daily revenue, z-score for anomaly detection.
        Returns DataFrame: date, revenue, zscore
        """
        rev = self.revenue_by_period(freq=freq)
        mu = rev.mean()
        sigma = rev.std(ddof=0)
        z = (rev - mu) / (sigma if sigma != 0 else 1.0)
        out = pd.DataFrame(
            {"period": rev.index.astype(str), "revenue": rev.values, "zscore": z.values}
        )
        return out

    # ---------- Export helpers ----------
    def summary(self) -> Dict[str, Any]:
        return {
            "total_revenue": float(round(self.total_revenue(), 2)),
            "total_profit": float(round(self.total_profit(), 2)),
            "orders_count": int(self.orders_count()),
            "avg_order_value": float(round(self.avg_order_value(), 2)),
            "profit_margin": float(round(self.profit_margin(), 4)),
        }

    def to_dashboard_payload(self) -> Dict[str, Any]:
        """
        Return a serializable payload including summary + top breakdowns for dashboards.
        """
        payload = {"summary": self.summary()}
        payload["top_categories"] = self.revenue_by_category(n=10).to_dict(
            orient="records"
        )
        payload["top_regions"] = self.revenue_by_region(n=10).to_dict(orient="records")

        neg_orders = self.negative_profit_orders(n=10).copy()
        if self.date_col in neg_orders.columns:
            neg_orders[self.date_col] = neg_orders[self.date_col].astype(str)

        payload["negative_profit_orders"] = neg_orders.to_dict(orient="records")
        return payload
