"""
Pull two months of hourly features from Feast and
build a training set whose label is the posted wait 60 minutes ahead.
"""

from pathlib import Path
import glob
import numpy as np
import pandas as pd
from feast import FeatureStore

# ── config ────────────────────────────────────────────────────────────────
HISTORY_START = "2025-04-01"
HISTORY_END   = "2025-05-31 23:30"
HORIZON_ROWS  = 2                         # 2 × 30-min rows → 60-min horizon
OUTFILE       = "training/hourly_train.parquet"
Path("training").mkdir(exist_ok=True)
# ───────────────────────────────────────────────────────────────────────────

store = FeatureStore("feature_repo")

# ── collect unique ride_id values from a few raw parquet parts ────────────
sample_parts = glob.glob("data/raw/*.parquet/park=*/*.parquet")
if not sample_parts:
    raise RuntimeError(
        "No raw parquet parts found – run the harvester or git pull binary-db-updates"
    )

ride_ids = set()
for p in sample_parts[:10]:                           # 10 parts is enough
    ride_ids.update(
        pd.read_parquet(p, columns=["ride_id"])["ride_id"].unique()
    )
ride_ids = sorted(ride_ids)
print(f"Found {len(ride_ids)} unique ride_id values")

# ── build entity dataframe (cartesian product) ────────────────────────────
timestamps = (
    pd.date_range(HISTORY_START, HISTORY_END, freq="30min", tz="UTC")
      .tz_localize(None)
)
entity_df = pd.DataFrame({
    "ride_id": np.repeat(ride_ids, len(timestamps)),
    "event_timestamp": np.tile(timestamps, len(ride_ids)),
})

# ── pull historical features from Feast ───────────────────────────────────
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "queue_weather_hourly:posted_wait",
    ],
).to_df()

# ── create 60-min-ahead target ────────────────────────────────────────────
training_df = training_df.sort_values(["ride_id", "event_timestamp"])
training_df["target"] = (
    training_df.groupby("ride_id")["posted_wait"].shift(-HORIZON_ROWS)
)
training_df = training_df.dropna(subset=["target"])

training_df.to_parquet(OUTFILE, index=False)
print(f"✅ wrote {len(training_df):,} rows → {OUTFILE}")
