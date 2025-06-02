import glob, os
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32
from datetime import timedelta

here = os.path.dirname(__file__)  # feature_repo/
queue_files = glob.glob(
    os.path.join(here, "..", "data", "raw", "*", "park=*",
                 "*.parquet")
    
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

ride = Entity(name="ride_id", join_keys=["ride_id"])

queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,
    schema=[
        Field(name="posted_wait", dtype=Int32),      # ✅ Int32, not ValueType
    ],
    online=False,
    source=QUEUE_RAW,
)

weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],            # no ride_id
    ttl=None,
    schema=[
        Field(name="temp_f",      dtype=Float32),    # ✅ Float32
        Field(name="precip_prob", dtype=Int32),
    ],
    online=False,
    source=WEATHER_RAW,
)
