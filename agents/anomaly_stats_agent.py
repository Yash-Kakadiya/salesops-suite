"""
agents/anomaly_stats_agent.py
Statistical Anomaly Detection Layer.
Fix: Handles sparse time-series data (min_periods=1).
"""

import sys
import json
import logging
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass, asdict

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class AnomalyRecord:
    anomaly_id: str
    level: str
    entity_id: str
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

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        if "Order Date" in self.df.columns:
            self.df["Order Date"] = pd.to_datetime(self.df["Order Date"])
            self.df = self.df.sort_values("Order Date")
        self.anomalies: List[AnomalyRecord] = []

    def _generate_id(self, date_str, entity, detector, score):
        clean_entity = str(entity).replace(" ", "_")
        return f"{detector}_{clean_entity}_{date_str}_s{int(score)}"

    def detect_global_zscore(
        self, target_col="Sales", window=30, threshold=3.0
    ) -> pd.DataFrame:
        logger.info(
            f"Running Global Z-Score Detector on {target_col} (w={window}, t={threshold})"
        )

        daily = self.df.groupby("Order Date")[target_col].sum().reset_index()

        # Global is dense (daily), so min_periods=5 is usually fine, but 1 is safer
        daily["mean"] = daily[target_col].rolling(window=window, min_periods=1).mean()
        daily["std"] = daily[target_col].rolling(window=window, min_periods=1).std()

        daily["zscore"] = (daily[target_col] - daily["mean"]) / (
            daily["std"].replace(0, 1)
        )

        outliers = daily[np.abs(daily["zscore"]) > threshold].copy()

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
        logger.info(f"Running Grouped IQR Detector on {group_col} (w={window}, k={k})")

        grouped = (
            self.df.groupby(["Order Date", group_col])[target_col].sum().reset_index()
        )

        outlier_frames = []

        for entity, group_df in grouped.groupby(group_col):
            group_df = group_df.sort_values("Order Date").copy()

            # This ensures we get stats even if recent history is gappy
            group_df["Q1"] = (
                group_df[target_col]
                .rolling(window=window, min_periods=1)
                .quantile(0.25)
            )
            group_df["Q3"] = (
                group_df[target_col]
                .rolling(window=window, min_periods=1)
                .quantile(0.75)
            )
            group_df["IQR"] = group_df["Q3"] - group_df["Q1"]

            raw_lower = group_df["Q1"] - (k * group_df["IQR"])

            # Ensure lower bound catches drops to near-zero even if variance is high
            group_df["lower"] = np.maximum(raw_lower, group_df["Q1"] * 0.25)

            group_df["upper"] = group_df["Q3"] + (k * group_df["IQR"])

            mask = (group_df[target_col] < group_df["lower"]) | (
                group_df[target_col] > group_df["upper"]
            )

            # Allow very small positive values (like our 99% drop) to be detected
            # Only ignore 0 or negatives if they aren't anomalies
            mask = mask & (group_df[target_col] >= 0)

            detected = group_df[mask].copy()

            if not detected.empty:
                outlier_frames.append(detected)

                for _, row in detected.iterrows():
                    date_str = row["Order Date"].strftime("%Y-%m-%d")

                    # Handle IQR Score calculation (avoid div/0)
                    iqr = row["IQR"] if row["IQR"] > 0 else 1.0
                    if row[target_col] > row["Q3"]:
                        dist = row[target_col] - row["Q3"]
                    else:
                        dist = row["Q1"] - row[target_col]

                    iqr_score = round(abs(dist) / iqr, 2)

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

    def detect_percentage_drop(
        self, target_col="Sales", group_col="Category", threshold=0.5, window=3
    ) -> pd.DataFrame:
        """Detect extreme percentage drops (e.g., 50% or more) within a group."""
        logger.info(
            f"Running Percentage Drop Detector on {group_col} (threshold={threshold*100}%)"
        )

        grouped = (
            self.df.groupby(["Order Date", group_col])[target_col].sum().reset_index()
        )

        outlier_frames = []

        for entity, group_df in grouped.groupby(group_col):
            group_df = group_df.sort_values("Order Date").copy()

            # Calculate previous day's value
            group_df["prev_value"] = group_df[target_col].shift(1)

            # Calculate percentage change
            group_df["pct_change"] = (
                group_df[target_col] - group_df["prev_value"]
            ) / group_df["prev_value"]

            # Detect extreme drops (negative percentage change beyond threshold)
            mask = group_df["pct_change"] < -threshold

            detected = group_df[mask].copy()

            if not detected.empty:
                outlier_frames.append(detected)

                for _, row in detected.iterrows():
                    date_str = row["Order Date"].strftime("%Y-%m-%d")
                    pct_drop = abs(row["pct_change"]) * 100
                    score = min(10.0, pct_drop / 10)

                    rec = AnomalyRecord(
                        anomaly_id=self._generate_id(
                            date_str, entity, "pct_drop", score
                        ),
                        level="category",
                        entity_id=entity,
                        period_start=date_str,
                        period_end=date_str,
                        metric=target_col,
                        value=float(row[target_col]),
                        expected=float(row["prev_value"]),
                        score=score,
                        detector="pct_drop",
                        reason=f"Extreme drop: {pct_drop:.1f}% from {row['prev_value']:.0f} to {row[target_col]:.0f}",
                        context={"pct_change": float(row["pct_change"])},
                    )

                    self.anomalies.append(rec)

        return pd.concat(outlier_frames) if outlier_frames else pd.DataFrame()

    def detect_percentage_spike(
        self, target_col="Sales", group_col="Category", threshold=0.5, window=3
    ) -> pd.DataFrame:
        """Detect extreme percentage spikes (e.g., 50% or more) within a group."""
        logger.info(
            f"Running Percentage Spike Detector on {group_col} (threshold={threshold*100}%)"
        )

        grouped = (
            self.df.groupby(["Order Date", group_col])[target_col].sum().reset_index()
        )

        outlier_frames = []

        for entity, group_df in grouped.groupby(group_col):
            group_df = group_df.sort_values("Order Date").copy()

            # Calculate previous day's value
            group_df["prev_value"] = group_df[target_col].shift(1)

            # Calculate percentage change
            group_df["pct_change"] = (
                group_df[target_col] - group_df["prev_value"]
            ) / group_df["prev_value"]

            # Detect extreme spikes (positive percentage change beyond threshold)
            mask = group_df["pct_change"] > threshold

            detected = group_df[mask].copy()

            if not detected.empty:
                outlier_frames.append(detected)

                for _, row in detected.iterrows():
                    date_str = row["Order Date"].strftime("%Y-%m-%d")
                    pct_rise = row["pct_change"] * 100
                    score = min(10.0, pct_rise / 20)

                    rec = AnomalyRecord(
                        anomaly_id=self._generate_id(
                            date_str, entity, "pct_spike", score
                        ),
                        level="category",
                        entity_id=entity,
                        period_start=date_str,
                        period_end=date_str,
                        metric=target_col,
                        value=float(row[target_col]),
                        expected=float(row["prev_value"]),
                        score=score,
                        detector="pct_spike",
                        reason=f"Extreme spike: +{pct_rise:.1f}% from {row['prev_value']:.0f} to {row[target_col]:.0f}",
                        context={"pct_change": float(row["pct_change"])},
                    )

                    self.anomalies.append(rec)

        return pd.concat(outlier_frames) if outlier_frames else pd.DataFrame()

    def get_anomalies_df(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(r) for r in self.anomalies])

    def save_payload(self, output_path: str):
        data = [asdict(r) for r in self.anomalies]
        data.sort(key=lambda x: x["score"], reverse=True)
        payload = {
            "count": len(data),
            "top_anomalies": data[:50],
            "all_anomalies": data,
        }
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"Saved {len(data)} anomalies to {output_path}")


if __name__ == "__main__":
    # ... (CLI remains the same)
    parser = argparse.ArgumentParser(description="Run Anomaly Detection")
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if not Path(args.snapshot).exists():
        sys.exit(1)
    df = pd.read_parquet(args.snapshot)
    agent = AnomalyStatAgent(df)
    agent.detect_global_zscore(30, 3.0)
    agent.detect_grouped_iqr("Region", "Sales", 14, 1.5)
    agent.detect_grouped_iqr("Category", "Sales", 14, 1.5)
    agent.save_payload(args.out)
