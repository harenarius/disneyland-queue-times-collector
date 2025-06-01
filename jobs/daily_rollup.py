# jobs/daily_rollup.py
"""
Merge yesterday’s hourly queue-time + weather rows into one day-level table.

Output: data/interim/daily.parquet   (partition-friendly for future use)
"""
from pathlib import Path

import pandas as pd

RAW = Path("data/raw")
OUT = Path("data/interim")
OUT.mkdir(parents=True, exist_ok=True)


def build():
    # ––– load yesterday’s raw parts –––
    yesterday = (pd.Timestamp.utcnow() - pd.Timedelta("1D")).strftime("%Y-%m-%d")
    q_parts = list(RAW.glob(f"{yesterday}.parquet/park=*/part*.parquet"))
    w_parts = list(RAW.glob(f"weather_{yesterday}.parquet"))

    if not q_parts or not w_parts:
        print("No raw files for", yesterday)
        return

    qdf = pd.concat(pd.read_parquet(p) for p in q_parts)
    wdf = pd.concat(pd.read_parquet(p) for p in w_parts)

    # ––– aggregate queue times –––
    qdf["date"] = qdf["timestamp"].dt.date
    daily = (
        qdf.groupby(["ride_id", "date"])["posted_wait"]
        .agg(mean_wait="mean", p90_wait=lambda s: s.quantile(0.9))
        .reset_index()
    )

    # ––– aggregate weather –––
    wdf["date"] = wdf["timestamp"].dt.date
    w_day = (
        wdf.groupby("date")
        .agg(high_temp=("temp_f", "max"), max_precip_prob=("precip_prob", "max"))
        .reset_index()
    )

    out = daily.merge(w_day, on="date", how="left")
    out_path = OUT / "daily.parquet"
    out.to_parquet(out_path, index=False)
    print("✅ wrote", len(out), "rows →", out_path)


if __name__ == "__main__":
    build()
