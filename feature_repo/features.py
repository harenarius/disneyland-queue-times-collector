from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType

# ── File sources ──────────────────────────────────────────────
QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw/*/park=*/*.parquet",     # ride-wait parts only
    timestamp_field="timestamp",
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/weather/weather_*.parquet",  # weather files
    timestamp_field="timestamp",
)

# ── entity ────────────────────────────────────────────────────
ride = Entity(
    name="ride_id",
    join_keys=["ride_id"],
    value_type=ValueType.INT32,                # required in newer Feast
)

# ── ride-wait FeatureView (posted_wait only) ──────────────────
queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,                                  # disable until tz cleaned
    schema=[
        Field(name="posted_wait", dtype=ValueType.INT32),
    ],
    online=False,
    source=QUEUE_RAW,
)

# ── weather FeatureView (no ride_id) ──────────────────────────
weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],                               # global features
    ttl=None,
    schema=[
        Field(name="temp_f",      dtype=ValueType.FLOAT),
        Field(name="precip_prob", dtype=ValueType.INT32),
    ],
    online=False,
    source=WEATHER_RAW,
)
