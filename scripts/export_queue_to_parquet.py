# scripts/export_queue_to_parquet.py

import sqlite3
import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
today = pd.Timestamp.utcnow().date()
day_path = RAW / f"{today}.parquet"
day_path.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect("queue_times.db")
df = pd.read_sql("SELECT * FROM queue_times", conn)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Split by park
for park in df["park"].unique():
    park_df = df[df["park"] == park]
    out = day_path / f"park={park}" / "part-0.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    park_df.to_parquet(out, index=False)

print(f"✅ Wrote queue data to {day_path}")
