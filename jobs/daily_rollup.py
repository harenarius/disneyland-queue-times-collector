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
    queue_files = [p for p in raw_path.rglob("*.parquet") if p.is_file()]
    if not queue_files:
        print(f"No queue data found for {day_str}")
        return

    dfs = []
    for p in queue_files:
        try:
            df = pd.read_parquet(p)
            df.columns = [str(c) for c in df.columns]  # normalize all column names to string
            if "park" in df.columns:
                df["park"] = df["park"].astype(str)     # normalize park type
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Failed to read {p.name}: {e}")

    if not dfs:
        print(f"[ERROR] No readable data found in {raw_path}")
        return

    qdf = pd.concat(dfs, ignore_index=True)
    qdf["timestamp"] = pd.to_datetime(qdf["timestamp"])
    qdf["time_bin"] = qdf["timestamp"].dt.floor("30min")
    qdf["date"] = qdf["timestamp"].dt.date

    grouped = (
        qdf.groupby(["date", "park", "ride", "time_bin"])
        .agg(
            wait_mean=("wait_time", "mean"),
            wait_std=("wait_time", "std"),
            sample_size=("wait_time", "count")
        )
        .reset_index()
    )

    # --- Load weather data (optional) ---
    weather_path = WEATHER / f"weather_{day_str}.parquet"
    if weather_path.exists():
        try:
            wdf = pd.read_parquet(weather_path)
            latest = wdf.sort_values("timestamp").iloc[-1]
            for col in ["temp_f", "precip_prob", "humidity", "wind_speed"]:
                grouped[col] = latest[col] if col in latest else None
        except Exception as e:
            print(f"[WARN] Failed to read or parse weather file: {e}")
            for col in ["temp_f", "precip_prob", "humidity", "wind_speed"]:
                grouped[col] = None
    else:
        print(f"[WARN] No weather file found for {day_str}, filling with nulls")
        for col in ["temp_f", "precip_prob", "humidity", "wind_speed"]:
            grouped[col] = None

    # --- Add holiday and day flags ---
    grouped["is_holiday"] = grouped["date"].isin(US_HOLIDAYS)
    grouped["day_of_week"] = pd.to_datetime(grouped["date"]).dt.day_name()
    grouped["is_weekend"] = grouped["day_of_week"].isin(["Saturday", "Sunday"])

    # --- Optional ride metadata ---
    if META.exists():
        meta = pd.read_parquet(META)
        grouped = grouped.merge(meta, on="ride", how="left")

    # --- Save output ---
    out_path = OUT / f"{day_str}.parquet"
    grouped.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(grouped)} rows to {out_path}")

if __name__ == "__main__":
    build()
