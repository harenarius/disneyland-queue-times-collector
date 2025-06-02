from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32
from datetime import timedelta

# ── File sources ──────────────────────────────────────────────
QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw/*/park=*/*.parquet",   # ✅ ride files only
    timestamp_field="timestamp",
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/weather/weather_*.parquet",   # ✅ weather lives here now
    timestamp_field="timestamp",
)

# ── entities ──────────────────────────────────────────────────
ride = Entity(name="ride_id", join_keys=["ride_id"])

# ── feature views ─────────────────────────────────────────────
# 1) ride-only view
queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,                           # disable TTL until tz issues solved
    schema=[Field("posted_wait", Int32)],
    online=False,
    source=QUEUE_RAW,
)

# 2) standalone weather view (one row per timestamp)
weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],                        # no ride_id
    ttl=None,
    schema=[
        Field("temp_f",      Float32),
        Field("precip_prob", Int32),
    ],
    online=False,
    source=WEATHER_RAW,
)
