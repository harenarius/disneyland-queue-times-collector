from pathlib import Path
import pandas as pd
import sqlite3

WEATHER_DIR = Path("data/weather")
WEATHER_DIR.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect("queue_times.db")
df = pd.read_sql("SELECT * FROM weather", conn)
df["timestamp"] = pd.to_datetime(df["timestamp"])

latest_day = df["timestamp"].dt.date.max()
day_df = df[df["timestamp"].dt.date == latest_day]

out_path = WEATHER_DIR / f"weather_{latest_day}.parquet"
day_df.to_parquet(out_path, index=False)

print(f"✅ Wrote weather to {out_path}")
