from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32
from datetime import timedelta

# ── File sources ──────────────────────────────────────────────
QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw/*.parquet/park=*/*.parquet",   # ride-wait files only
    timestamp_field="timestamp",
    hive_partitioning=True,                          # ← tell Feast about park=…
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/raw/weather_*.parquet",            # weather files (no hive)
    timestamp_field="timestamp",
)

# ── entity ────────────────────────────────────────────────────
ride = Entity(name="ride_id", join_keys=["ride_id"])

# ── feature view ──────────────────────────────────────────────
queue_weather_hourly = FeatureView(
    name="queue_weather_hourly",
    entities=[ride],
    ttl=timedelta(days=90),
    schema=[
        Field(name="posted_wait",  dtype=Int32),
        Field(name="temp_f",       dtype=Float32),
        Field(name="precip_prob",  dtype=Int32),
    ],
    online=False,
    source=QUEUE_RAW,
)
