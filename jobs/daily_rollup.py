# jobs/daily_rollup.py
"""
Aggregate queue wait times and weather into 30-minute ride-level summaries.
Output: data/rollup/YYYY-MM-DD.parquet
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import holidays

# --- Paths ---
RAW = Path("data/raw")
WEATHER = Path("data/weather")
META = Path("data/meta/rides.parquet")
OUT = Path("data/rollup")
OUT.mkdir(parents=True, exist_ok=True)

# --- Holiday setup ---
US_HOLIDAYS = holidays.US()

def build():
    date = (datetime.utcnow() - timedelta(days=1)).date()
    day_str = date.strftime("%Y-%m-%d")
    raw_path = RAW / f"{day_str}.parquet"

    # --- Load ride data ---
    queue_files = list(raw_path.rglob("*.parquet"))
    if not queue_files:
        print(f"No queue data found for {day_str}")
        return
    qdf = pd.concat(pd.read_parquet(p) for p in queue_files)
    qdf["timestamp"] = pd.to_datetime(qdf["timestamp"])
    qdf["time_bin"] = qdf["timestamp"].dt.floor("30min")
    qdf["date"] = qdf["timestamp"].dt.date

    grouped = (
        qdf.groupby(["date", "park", "ride_id", "ride_name", "time_bin"])
        .agg(
            wait_mean=("wait_time", "mean"),
            wait_std=("wait_time", "std"),
            sample_size=("wait_time", "count")
        )
        .reset_index()
    )

    # --- Load weather data ---
    weather_path = WEATHER / f"weather_{day_str}.parquet"
    if not weather_path.exists():
        print(f"No weather file found for {day_str}")
        return
    wdf = pd.read_parquet(weather_path)
    latest = wdf.sort_values("timestamp").iloc[-1]
    for col in ["temp_f", "precip_prob", "humidity", "wind_speed"]:
        grouped[col] = latest[col]

    # --- Add holiday and day flags ---
    grouped["is_holiday"] = grouped["date"].isin(US_HOLIDAYS)
    grouped["day_of_week"] = pd.to_datetime(grouped["date"]).dt.day_name()
    grouped["is_weekend"] = grouped["day_of_week"].isin(["Saturday", "Sunday"])

    # --- Optional ride metadata ---
    if META.exists():
        meta = pd.read_parquet(META)
        grouped = grouped.merge(meta, on="ride_id", how="left")

    # --- Save output ---
    out_path = OUT / f"{day_str}.parquet"
    grouped.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(grouped)} rows to {out_path}")

if __name__ == "__main__":
    build()
