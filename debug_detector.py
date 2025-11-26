import pandas as pd
import numpy as np
from agents.anomaly_stats_agent import AnomalyStatAgent

df = pd.read_parquet("data/test_labels/synthetic_sales.parquet")
df["Order Date"] = pd.to_datetime(df["Order Date"])

# Check Technology data around the key dates
for target_date in ["2014-03-18", "2017-03-23"]:
    print(f"\n=== {target_date} ===")
    tech_daily = (
        df[df["Category"] == "Technology"]
        .groupby("Order Date")["Sales"]
        .sum()
        .sort_index()
    )
    target_dt = pd.to_datetime(target_date)

    if target_dt in tech_daily.index:
        print(f"Tech sales on {target_date}: {tech_daily[target_dt]:.2f}")

    # Show range
    start = target_dt - pd.Timedelta(days=3)
    end = target_dt + pd.Timedelta(days=3)
    print("Range around date:")
    print(tech_daily[start:end])

# Now test the detector
print("\n=== DETECTOR OUTPUT ===")
agent = AnomalyStatAgent(df)
agent.detect_global_zscore(threshold=3.0)
agent.detect_grouped_iqr(group_col="Region", k=2.0)
agent.detect_grouped_iqr(group_col="Category", k=2.0)

all_detected = agent.get_anomalies_df()
print(f"Total anomalies detected: {len(all_detected)}")

# Check if we detect Technology on those dates
for target_date in ["2014-03-18", "2017-03-23"]:
    tech_detected = all_detected[
        (all_detected["entity_id"] == "Technology")
        & (all_detected["period_start"].astype(str).str.startswith(target_date))
    ]
    if not tech_detected.empty:
        print(f"FOUND Technology anomaly on {target_date}")
        print(tech_detected[["period_start", "entity_id", "detector"]])
    else:
        print(f"NOT FOUND Technology anomaly on {target_date}")
