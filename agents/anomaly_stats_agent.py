"""
agents/anomaly_stats_agent.py

Statistical Anomaly Detection Layer for SalesOps Suite.
Implements Z-Score, Rolling Deviation, and IQR detectors with robust scoring.
"""

import sys
import json
import logging
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

# Configure Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class AnomalyRecord:
    """Standard schema for an anomaly event."""

    anomaly_id: str
    level: str  # daily, category, product, region
    entity_id: str  # "Global", "West", "Technology"
    period_start: str
    period_end: str
    metric: str
    value: float
    expected: float
    score: float
    detector: str
    reason: str
    context: Dict[str, Any]


class AnomalyStatAgent:
    """
    Statistical Agent that finds outliers in time-series data.
    Does not use LLMs. Uses robust statistics.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Args:
            df: DataFrame with 'Order Date' and metric columns.
        """
        self.df = df.copy()
        if "Order Date" in self.df.columns:
            self.df["Order Date"] = pd.to_datetime(self.df["Order Date"])
            self.df = self.df.sort_values("Order Date")

        # Output buffer
        self.anomalies: List[AnomalyRecord] = []

    def _generate_id(self, date_str, entity, detector, score):
        """Creates a unique ID for the anomaly."""
        clean_entity = str(entity).replace(" ", "_")
        return f"{detector}_{clean_entity}_{date_str}_s{int(score)}"

    def detect_global_zscore(
        self, target_col="Sales", window=30, threshold=3.0
    ) -> pd.DataFrame:
        """
        Run Z-Score detection on the global aggregated time series.
        """
        logger.info(
            f"Running Global Z-Score Detector on {target_col} (w={window}, t={threshold})"
        )

        # 1. Aggregate to Daily
        daily = self.df.groupby("Order Date")[target_col].sum().reset_index()

        # 2. Calculate Stats
        daily["mean"] = daily[target_col].rolling(window=window, min_periods=1).mean()
        daily["std"] = daily[target_col].rolling(window=window, min_periods=1).std()

        # 3. Compute Z-Score (Handle div by zero)
        daily["zscore"] = (daily[target_col] - daily["mean"]) / (
            daily["std"].replace(0, 1)
        )

        # 4. Filter
        outliers = daily[np.abs(daily["zscore"]) > threshold].copy()

        # 5. Convert to Standard Records
        for _, row in outliers.iterrows():
            date_str = row["Order Date"].strftime("%Y-%m-%d")
            score = round(abs(row["zscore"]), 2)

            rec = AnomalyRecord(
                anomaly_id=self._generate_id(date_str, "Global", "zscore", score),
                level="global",
                entity_id="All_Regions",
                period_start=date_str,
                period_end=date_str,
                metric=target_col,
                value=float(row[target_col]),
                expected=float(round(row["mean"], 2)),
                score=score,
                detector="zscore",
                reason=f"Spike detected (Z={score})",
                context={
                    "window_mean": float(round(row["mean"], 2)),
                    "window_std": float(round(row["std"], 2)),
                    "threshold": threshold,
                },
            )
            self.anomalies.append(rec)

        return outliers

    def detect_grouped_iqr(
        self, group_col="Region", target_col="Sales", window=14, k=1.5
    ) -> pd.DataFrame:
        """
        Run IQR (Inter-Quartile Range) detection per group.
        Good for skewed data like B2B sales.
        """
        logger.info(f"Running Grouped IQR Detector on {group_col} (w={window}, k={k})")

        # 1. Aggregate
        grouped = (
            self.df.groupby(["Order Date", group_col])[target_col].sum().reset_index()
        )

        outlier_frames = []

        # 2. Iterate Groups (Safer for rolling windows than transform on small groups)
        for entity, group_df in grouped.groupby(group_col):
            group_df = group_df.sort_values("Order Date").copy()

            # Rolling Quartiles
            group_df["Q1"] = (
                group_df[target_col]
                .rolling(window=window, min_periods=5)
                .quantile(0.25)
            )
            group_df["Q3"] = (
                group_df[target_col]
                .rolling(window=window, min_periods=5)
                .quantile(0.75)
            )
            group_df["IQR"] = group_df["Q3"] - group_df["Q1"]

            # Bounds
            group_df["lower"] = group_df["Q1"] - (k * group_df["IQR"])
            group_df["upper"] = group_df["Q3"] + (k * group_df["IQR"])

            # Detect
            mask = (group_df[target_col] < group_df["lower"]) | (
                group_df[target_col] > group_df["upper"]
            )
            # Ignore trivial values (e.g. 0 sales is not an anomaly if lower bound is negative)
            mask = mask & (group_df[target_col] > 10)

            detected = group_df[mask].copy()

            if not detected.empty:
                outlier_frames.append(detected)

                # Create Records
                for _, row in detected.iterrows():
                    date_str = row["Order Date"].strftime("%Y-%m-%d")
                    # IQR Score: How many IQRs away is it?
                    dist = (
                        abs(row[target_col] - row["Q3"])
                        if row[target_col] > row["Q3"]
                        else abs(row["Q1"] - row[target_col])
                    )
                    iqr_score = round(dist / (row["IQR"] if row["IQR"] > 0 else 1), 2)

                    rec = AnomalyRecord(
                        anomaly_id=self._generate_id(
                            date_str, entity, "iqr", iqr_score
                        ),
                        level=group_col.lower(),
                        entity_id=str(entity),
                        period_start=date_str,
                        period_end=date_str,
                        metric=target_col,
                        value=float(row[target_col]),
                        expected=(
                            float(round(row["Q3"], 2))
                            if row[target_col] > row["Q3"]
                            else float(round(row["Q1"], 2))
                        ),
                        score=iqr_score,
                        detector="iqr",
                        reason=f"Outside Tukey Fence (Score={iqr_score})",
                        context={
                            "Q1": float(round(row["Q1"], 2)),
                            "Q3": float(round(row["Q3"], 2)),
                            "IQR": float(round(row["IQR"], 2)),
                        },
                    )
                    self.anomalies.append(rec)

        return pd.concat(outlier_frames) if outlier_frames else pd.DataFrame()

    def get_anomalies_df(self) -> pd.DataFrame:
        """Returns current anomalies as a DataFrame."""
        return pd.DataFrame([asdict(r) for r in self.anomalies])

    def save_payload(self, output_path: str):
        """
        Saves the consolidated anomaly payload to JSON.
        """
        data = [asdict(r) for r in self.anomalies]
        # Sort by score descending (High Priority first)
        data.sort(key=lambda x: x["score"], reverse=True)

        payload = {
            "count": len(data),
            "top_anomalies": data[:50],  # Top 50 for LLM context
            "all_anomalies": data,
        }

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Saved {len(data)} anomalies to {output_path}")


# --- CLI Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Anomaly Detection")
    parser.add_argument(
        "--snapshot", required=True, help="Path to cleaned parquet file"
    )
    parser.add_argument("--out", required=True, help="Path to output JSON payload")

    args = parser.parse_args()

    # Load
    if not Path(args.snapshot).exists():
        print(f"Error: {args.snapshot} not found.")
        sys.exit(1)

    df = pd.read_parquet(args.snapshot)
    agent = AnomalyStatAgent(df)

    # Run Detectors
    agent.detect_global_zscore(window=30, threshold=3.0)
    agent.detect_grouped_iqr(group_col="Region", window=14, k=1.5)
    agent.detect_grouped_iqr(group_col="Category", window=14, k=1.5)

    # Save
    agent.save_payload(args.out)
    print(f"Done. Anomalies saved to {args.out}")
