# feature_repo/features.py
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32
from datetime import timedelta

QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw",          # ← real folder, no wildcards
    timestamp_field="timestamp",
    # (no hive_partitioning flag in this Feast version)
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/raw/weather_*.parquet",   # weather files kept separate
    timestamp_field="timestamp",
)

ride = Entity(name="ride_id", join_keys=["ride_id"])

queue_weather_hourly = FeatureView(
    name="queue_weather_hourly",
    entities=[ride],
    ttl=timedelta(days=90),
    schema=[
        Field(name="posted_wait", dtype=Int32),
        Field(name="temp_f",      dtype=Float32),
        Field(name="precip_prob", dtype=Int32),
    ],
    online=False,
    source=QUEUE_RAW,
)
