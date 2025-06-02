from feast import Entity, FeatureView, Feature, FileSource      # ← Feature!
from feast.types import Float32, Int32
from datetime import timedelta

QUEUE_RAW = FileSource(
    name="queue_raw",
    path="../data/raw/*/park=*/*.parquet",
    timestamp_field="timestamp",
)

ride = Entity(name="ride_id", join_keys=["ride_id"])

queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,
    schema=[
        Feature(name="posted_wait", dtype=Int32),    # ← use Feature
    ],
    online=False,
    source=QUEUE_RAW,
)

weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],            # no ride_id column
    ttl=None,
    schema=[
        Feature(name="temp_f",      dtype=Float32),
        Feature(name="precip_prob", dtype=Int32),
    ],
    online=False,
    source=FileSource(
        name="weather_raw",
        path="../data/weather/weather_*.parquet",
        timestamp_field="timestamp",
    ),
)
