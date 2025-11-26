import pandas as pd

df = pd.read_parquet("data/test_labels/synthetic_sales.parquet")
df["Order Date"] = pd.to_datetime(df["Order Date"])

# Group by date and category
tech_daily = (
    df[df["Category"] == "Technology"].groupby("Order Date")["Sales"].sum().sort_index()
)

for target_date in ["2014-03-18", "2017-03-23"]:
    target_dt = pd.to_datetime(target_date)
    if target_dt in tech_daily.index:
        prev_idx = tech_daily.index.get_loc(target_dt) - 1
        if prev_idx >= 0:
            prev_date = tech_daily.index[prev_idx]
            prev_val = tech_daily.iloc[prev_idx]
            curr_val = tech_daily[target_dt]
            pct_change = (curr_val - prev_val) / prev_val
            print(
                f"{target_date}: {prev_val:.2f} -> {curr_val:.2f} ({pct_change*100:.1f}%)"
            )
        else:
            print(f"{target_date}: No previous day")
