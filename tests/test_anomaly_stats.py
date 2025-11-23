import sys
import os
import pytest
import pandas as pd
import numpy as np

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from agents.anomaly_stats_agent import AnomalyStatAgent


@pytest.fixture
def synthetic_data():
    """Creates 100 days of steady sales with ONE massive spike."""
    # Set seed for reproducibility
    np.random.seed(42)

    dates = pd.date_range(start="2024-01-01", periods=100)
    sales = np.random.normal(100, 10, 100)  # Mean 100, Std 10

    df = pd.DataFrame({"Order Date": dates, "Sales": sales, "Region": "North"})

    # Inject Spike at index 90 (Value 500 vs Mean 100)
    df.loc[90, "Sales"] = 500
    return df


def test_zscore_detection(synthetic_data):
    agent = AnomalyStatAgent(synthetic_data)

    # FIX: Increased window from 10 to 30.
    # With window=10, the max possible Z-score is ~2.84 (mathematically).
    # With window=30, the max possible Z-score is >5.0.
    anomalies_df = agent.detect_global_zscore(window=30, threshold=3.0)

    # Assertions
    assert not anomalies_df.empty, "Should detect the spike"

    # Verify it found the specific spike we injected
    spike_row = anomalies_df[anomalies_df["Sales"] == 500]
    assert not spike_row.empty
    assert spike_row.iloc[0]["zscore"] > 3.0

    # Check Payload format
    records = agent.get_anomalies_df()
    zscore_records = records[records["detector"] == "zscore"]
    assert len(zscore_records) >= 1
    assert zscore_records.iloc[0]["score"] > 3.0


def test_grouped_iqr(synthetic_data):
    agent = AnomalyStatAgent(synthetic_data)

    # Run grouped detection (Region='North')
    agent.detect_grouped_iqr(group_col="Region", window=20, k=1.5)

    records = agent.get_anomalies_df()
    iqr_records = records[records["detector"] == "iqr"]

    assert len(iqr_records) >= 1
    assert iqr_records.iloc[0]["level"] == "region"
    assert iqr_records.iloc[0]["entity_id"] == "North"


if __name__ == "__main__":
    # Allow manual run
    try:
        dates = pd.date_range(start="2024-01-01", periods=100)
        sales = np.random.normal(100, 10, 100)
        df = pd.DataFrame({"Order Date": dates, "Sales": sales, "Region": "North"})
        df.loc[90, "Sales"] = 500

        agent = AnomalyStatAgent(df)
        res = agent.detect_global_zscore(window=30, threshold=3.0)
        print(f"Manual Test: Found {len(res)} anomalies. Max Z: {res['zscore'].max()}")
    except Exception as e:
        print(e)
