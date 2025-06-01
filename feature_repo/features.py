from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource  # ← FileSource lives here
from feast.types import Float32, Int32

# ---------- raw hourly sources ----------
QUEUE_RAW = FileSource(
    name="queue_raw",
    path="data/raw/*/*.parquet",  # hourly parts
    timestamp_field="timestamp",
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="data/raw/weather_*.parquet",
    timestamp_field="timestamp",
)

# ---------- entity ----------
ride = Entity(name="ride_id", join_keys=["ride_id"])

# ---------- feature view (joined hourly queue + weather) ----------
queue_weather_hourly = FeatureView(
    name="queue_weather_hourly",
    entities=[ride],
    ttl=timedelta(days=90),
    schema=[
        Field(name="posted_wait", dtype=Int32),
        Field(name="temp_f", dtype=Float32),
        Field(name="precip_prob", dtype=Int32),
    ],
    online=False,  # offline-only for now
    # you can point directly at QUEUE_RAW for a minimal MVP,
    # or keep this placeholder and perform a join upstream.
    source=QUEUE_RAW,
)
