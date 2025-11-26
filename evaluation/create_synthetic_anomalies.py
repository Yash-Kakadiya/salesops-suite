"""
evaluation/create_synthetic_anomalies.py
Generates a "Golden Dataset" by injecting known anomalies into the baseline data.
Fix: Dynamically finds peak dates to ensure dips are statistically significant.
"""

import pandas as pd
import numpy as np
import json
import argparse
import sys
import os
from pathlib import Path
from typing import List, Dict


class SyntheticInjector:
    def __init__(self, input_path: str, output_dir: str):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if not self.input_path.exists():
            raise FileNotFoundError(f"Input data not found at {self.input_path}")

        self.df = pd.read_parquet(self.input_path)

        if "Order Date" in self.df.columns:
            self.df["Order Date"] = pd.to_datetime(self.df["Order Date"])
            self.df = self.df.sort_values("Order Date")

        self.labels: List[Dict] = []

    def find_peak_date(self, category: str = None) -> str:
        """Finds the date with the highest sales (Global or per Category)."""
        if category:
            mask = self.df["Category"] == category
            subset = self.df[mask]
        else:
            subset = self.df

        # Group by date to handle multiple orders same day
        daily = subset.groupby("Order Date")["Sales"].sum()
        peak_date = daily.idxmax()
        return peak_date.strftime("%Y-%m-%d")

    def find_available_date(self, category: str, target_date: str = None) -> str:
        """Find the nearest available date in data for a given category."""
        if target_date:
            try:
                target = pd.to_datetime(target_date)
                # Check if data exists on that date
                if (
                    (self.df["Order Date"] == target)
                    & (self.df["Category"] == category)
                ).any():
                    return target_date
                # Otherwise find nearest date after target
                available = self.df[self.df["Category"] == category].copy()
                available["Order Date"] = pd.to_datetime(available["Order Date"])
                available = available[available["Order Date"] >= target].sort_values(
                    "Order Date"
                )
                if not available.empty:
                    return available.iloc[0]["Order Date"].strftime("%Y-%m-%d")
            except:
                pass
        # Fallback to peak date
        return self.find_peak_date(category)

    def inject_global_spike(self, date_str: str = None, factor: float = 3.0):
        """Multiplies all sales on a given day by factor."""
        if not date_str:
            # Pick a random valid date if none provided
            date_str = self.df["Order Date"].sample(1).dt.strftime("%Y-%m-%d").iloc[0]

        target_date = pd.to_datetime(date_str)
        mask = self.df["Order Date"] == target_date

        if not mask.any():
            print(f"⚠️ Warning: No data found for {date_str}, skipping injection.")
            return

        original_sales = self.df.loc[mask, "Sales"].sum()
        self.df.loc[mask, "Sales"] *= factor
        new_sales = self.df.loc[mask, "Sales"].sum()

        self.labels.append(
            {
                "anomaly_id": f"syn_global_spike_{date_str}",
                "date": date_str,
                "type": "global_spike",
                "factor": factor,
                "original_value": float(original_sales),
                "new_value": float(new_sales),
                "level": "global",
                "entity": "All_Regions",
            }
        )
        print(f"💉 Injected Global Spike on {date_str} (x{factor})")

    def inject_category_dip(
        self, category: str, factor: float = 0.1, date_str: str = None
    ):
        """Reduces sales for a category. Defaults to Peak Date for max visibility."""
        if not date_str:
            date_str = self.find_peak_date(category)

        target_date = pd.to_datetime(date_str)
        mask = (self.df["Order Date"] == target_date) & (
            self.df["Category"] == category
        )

        if not mask.any():
            print(f"⚠️ Warning: No {category} data for {date_str}, skipping injection.")
            return

        original = self.df.loc[mask, "Sales"].sum()
        self.df.loc[mask, "Sales"] *= factor
        new_val = self.df.loc[mask, "Sales"].sum()

        self.labels.append(
            {
                "anomaly_id": f"syn_dip_{category}_{date_str}",
                "date": date_str,
                "type": "category_dip",
                "factor": factor,
                "original_value": float(original),
                "new_value": float(new_val),
                "level": "category",
                "entity": category,
            }
        )
        print(
            f"💉 Injected {category} Dip on {date_str} (x{factor}) [Original: {original:.0f} -> New: {new_val:.0f}]"
        )

    def save(self):
        data_out = self.output_dir / "synthetic_sales.parquet"
        self.df.to_parquet(data_out)

        labels_out = self.output_dir / "anomalies_gold.jsonl"
        with open(labels_out, "w") as f:
            for label in self.labels:
                f.write(json.dumps(label) + "\n")

        print(f"\n✅ Successfully generated Golden Dataset.")
        print(f"   Test Data: {data_out}")
        print(f"   Gold Labels: {labels_out} ({len(self.labels)} anomalies)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="../data/processed/superstore_clean.parquet")
    parser.add_argument("--out", default="../data/test_labels")
    args = parser.parse_args()

    try:
        injector = SyntheticInjector(args.input, args.out)

        # 1. Global Spike (Fixed Date known to have data)
        injector.inject_global_spike("2016-11-15", factor=5.0)

        # 2. Category Dips - on dates where data definitely exists
        # Use find_peak_date to ensure we dip from a high point
        tech_peak_1 = injector.find_peak_date("Technology")
        injector.inject_category_dip(
            category="Technology", factor=0.01, date_str=tech_peak_1
        )

        # For second dip, find the second highest day
        tech_daily = (
            injector.df[injector.df["Category"] == "Technology"]
            .groupby("Order Date")["Sales"]
            .sum()
        )
        # Get top 2 days, use the second one
        top_dates = tech_daily.nlargest(2).index.tolist()
        if len(top_dates) >= 2:
            tech_peak_2 = top_dates[1].strftime("%Y-%m-%d")
        else:
            tech_peak_2 = tech_peak_1  # Fallback to peak if only one date
        injector.inject_category_dip(
            category="Technology", factor=0.1, date_str=tech_peak_2
        )

        injector.save()

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
