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
    dates = pd.date_range(start="2024-01-01", periods=100)
    sales = np.random.normal(100, 10, 100)  # Mean 100, Std 10

    df = pd.DataFrame({"Order Date": dates, "Sales": sales, "Region": "North"})

    # Inject Spike at index 90
    df.loc[90, "Sales"] = 500  # 40 std devs away!
    return df


def test_zscore_detection(synthetic_data):
    agent = AnomalyStatAgent(synthetic_data)

    # Run detection
    anomalies_df = agent.detect_global_zscore(window=10, threshold=3.0)

    # Assertions
    assert not anomalies_df.empty, "Should detect the spike"
    spike_row = anomalies_df.iloc[0]
    assert spike_row["Sales"] == 500.0
    assert spike_row["zscore"] > 3.0

    # Check Payload format
    records = agent.get_anomalies_df()
    assert len(records) == 1
    assert records.iloc[0]["detector"] == "zscore"
    assert records.iloc[0]["score"] > 10.0


def test_grouped_iqr(synthetic_data):
    agent = AnomalyStatAgent(synthetic_data)

    # Run grouped detection (Region='North')
    agent.detect_grouped_iqr(group_col="Region", window=10, k=1.5)

    records = agent.get_anomalies_df()
    assert len(records) >= 1
    assert records.iloc[0]["level"] == "region"
    assert records.iloc[0]["entity_id"] == "North"


if __name__ == "__main__":
    # Allow running this file directly
    try:
        # Create dummy data manually to run without pytest command if needed
        dates = pd.date_range(start="2024-01-01", periods=100)
        sales = np.random.normal(100, 10, 100)
        df = pd.DataFrame({"Order Date": dates, "Sales": sales, "Region": "North"})
        df.loc[90, "Sales"] = 500

        agent = AnomalyStatAgent(df)
        res = agent.detect_global_zscore()
        print(
            f"Manual Test: Found {len(res)} anomalies. Top score: {res['zscore'].max()}"
        )
    except Exception as e:
        print(e)
