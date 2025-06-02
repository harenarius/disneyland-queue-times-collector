# feature_repo/features.py
from feast import Entity, FeatureView, Feature, FileSource, ValueType
from datetime import timedelta

# ── File sources ──────────────────────────────────────────────
QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw/*/park=*/*.parquet",
    timestamp_field="timestamp",
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/weather/weather_*.parquet",
    timestamp_field="timestamp",
)

# ── entities ──────────────────────────────────────────────────
ride = Entity(
    name="ride_id",
    join_keys=["ride_id"],
    value_type=ValueType.INT32,              # ← add this
)

# ── feature views ─────────────────────────────────────────────
queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,
    schema=[
        Feature(name="posted_wait", dtype=ValueType.INT32),   # ✔ correct enum
    ],
    online=False,
    source=QUEUE_RAW,
)

weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],        # no ride_id
    ttl=None,
    schema=[
        Feature(name="temp_f",      dtype=ValueType.FLOAT),
        Feature(name="precip_prob", dtype=ValueType.INT32),
    ],
    online=False,
    source=WEATHER_RAW,
)
