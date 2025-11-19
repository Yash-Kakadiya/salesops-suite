import pandas as pd
import numpy as np


class FeatureEngineer:
    """
    Handles feature engineering for SalesOps data.
    Adds time-series features, rolling metrics, and lag indicators.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

        # Ensure data is sorted by date for rolling calcs
        if "Order Date" in self.df.columns:
            self.df = self.df.sort_values("Order Date")

    def add_time_features(self):
        """Extracts year, month, quarter, day_of_week from Order Date."""
        if "Order Date" not in self.df.columns:
            raise ValueError("Order Date column missing")

        self.df["Order Year"] = self.df["Order Date"].dt.year
        self.df["Order Month"] = self.df["Order Date"].dt.month
        self.df["Order Quarter"] = self.df["Order Date"].dt.quarter
        self.df["Day of Week"] = self.df["Order Date"].dt.day_name()
        return self.df

    def add_rolling_metrics(self, target_col="Sales", window=3, group_by=None):
        """
        Adds rolling averages (e.g., 3-month moving average).
        Args:
            target_col: Column to calculate rolling mean on (e.g., 'Sales')
            window: Number of periods (rows) to look back
            group_by: If provided (e.g., 'Region'), calculates rolling mean per group
        """
        col_name = f"{target_col}_Rolling_{window}"

        if group_by:
            # Group by region/category first, then apply rolling
            self.df[col_name] = self.df.groupby(group_by)[target_col].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
        else:
            # Global rolling average
            self.df[col_name] = (
                self.df[target_col].rolling(window=window, min_periods=1).mean()
            )

        return self.df

    def add_lag_features(self, target_col="Sales", lag=1):
        """Adds previous period's value (Lag-1) to detect growth/decline."""
        col_name = f"{target_col}_Lag_{lag}"
        self.df[col_name] = self.df[target_col].shift(lag)

        # Calculate Growth Rate
        growth_col = f"{target_col}_Growth_Pct"
        self.df[growth_col] = (
            (self.df[target_col] - self.df[col_name]) / self.df[col_name] * 100
        )

        # Fill NaN created by shifting with 0
        self.df[col_name] = self.df[col_name].fillna(0)
        self.df[growth_col] = self.df[growth_col].fillna(0)

        return self.df

    def get_engineered_data(self):
        return self.df
