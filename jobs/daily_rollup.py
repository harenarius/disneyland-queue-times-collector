# jobs/30min_rollup.py
"""
Aggregate ride wait time + weather data into 30-minute intervals.
Outputs per-ride, per-day, per-30min stats into data/rollup/YYYY-MM-DD.parquet
"""

from pathlib import Path
import pandas as pd
import holidays

RAW = Path("data/raw")
WEATHER = Path("data/weather")
META = Path("data/meta/rides.parquet")
OUT = Path("data/rollup")
OUT.mkdir(parents=True, exist_ok=True)

US_HOLIDAYS = holidays.US()

def build():
    # Get yesterday's date
    date = (pd.Timestamp.utcnow() - pd.Timedelta("1D")).date()
    day_str = date.strftime("%Y-%m-%d")

    # Load ride wait time parts
    q_parts = list(RAW.glob(f"{day_str}.parquet/park=*/part*.parquet"))
    if not q_parts:
        print("No queue data found.")
        return
    qdf = pd.concat(pd.read_parquet(p) for p in q_parts)
    qdf["timestamp"] = pd.to_datetime(qdf["timestamp"])
    qdf["time_bin"] = qdf["timestamp"].dt.floor("30T")
    qdf["date"] = qdf["timestamp"].dt.date

    # Aggregate to 30-min bins
    ride_summary = (
        qdf.groupby(["date", "park", "ride_id", "ride_name", "time_bin"])
        .agg(
            wait_mean=("wait_time", "mean"),
            wait_std=("wait_time", "std"),
            sample_size=("wait_time", "count"),
        )
        .reset_index()
    )

    # Load weather
    weather_path = WEATHER / f"weather_{day_str}.parquet"
    if not weather_path.exists():
        print("No weather file found.")
        return
    wdf = pd.read_parquet(weather_path)
    latest_weather = wdf.sort_values("timestamp").iloc[-1:]

    # Broadcast weather to all rows
    for col in ["temp_f", "precip_prob", "humidity", "wind_speed"]:
        ride_summary[col] = latest_weather.iloc[0][col]

    # Add holiday, day of week, weekend
    ride_summary["is_holiday"] = ride_summary["date"].isin(US_HOLIDAYS)
    ride_summary["day_of_week"] = pd.to_datetime(ride_summary["date"]).dt.day_name()
    ride_summary["is_weekend"] = ride_summary["day_of_week"].isin(["Saturday", "Sunday"])

    # Join with ride metadata (if available)
    meta_path = META
    if meta_path.exists():
        metadata = pd.read_parquet(meta_path)
        ride_summary = ride_summary.merge(metadata, on="ride_id", how="left")

    # Save
    out_path = OUT / f"{day_str}.parquet"
    ride_summary.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(ride_summary)} rows to {out_path}")

if __name__ == "__main__":
    build()
