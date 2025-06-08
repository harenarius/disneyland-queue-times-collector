# jobs/daily_rollup.py

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import holidays
import fsspec

# --- Paths ---
RAW = "s3://disney-queue-times/raw"
WEATHER = "s3://disney-queue-times/weather"
META = Path("data/meta/rides.parquet")
OUT = Path("data/rollup")
OUT.mkdir(parents=True, exist_ok=True)

# --- Holiday setup ---
US_HOLIDAYS = holidays.US()

def build():
    date = (datetime.utcnow() - timedelta(days=1)).date()
    day_str = date.strftime("%Y-%m-%d")

    # --- Load ride data from S3 ---
    raw_path = f"{RAW}/{day_str}.parquet/"
    queue_files = fsspec.open_files(f"{raw_path}park=*/*.parquet")

    if not queue_files:
        print(f"No queue data found for {day_str}")
        return

    dfs = []
    for f in queue_files:
        try:
            with f.open() as file:
                df = pd.read_parquet(file)
                df.columns = [str(c) for c in df.columns]
                if {"park", "ride", "wait_time", "timestamp"}.issubset(df.columns):
                    df["park"] = df["park"].astype(str)
                    dfs.append(df)
                else:
                    print(f"[WARN] Skipping {f.path} — missing required columns")
        except Exception as e:
            print(f"[WARN] Failed to read {f.path}: {e}")

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

    # --- Load weather data from S3 ---
    weather_path = f"{WEATHER}/weather_{day_str}.parquet"
    fs = fsspec.filesystem("s3")
    if fs.exists(weather_path):
        try:
            wdf = pd.read_parquet(weather_path)
            latest = wdf.sort_values("timestamp").iloc[-1]
            column_map = {
                "temperature": "temp_f",
                "humidity": "humidity",
                "wind_speed": "wind_speed",
                "precip_prob": "precip_prob"
            }
            for src, dest in column_map.items():
                grouped[dest] = latest[src] if src in latest else None
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

    grouped["estimated_throughput"] = grouped["sample_size"] * 10

    # --- Save output ---
    out_path = OUT / f"{day_str}.parquet"
    grouped.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(grouped)} rows to {out_path}")

if __name__ == "__main__":
    build()
