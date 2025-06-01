from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.file_source import ParquetFileSource
from feast.types import Float32, Int32

QUEUE_RAW = ParquetFileSource(
    name="queue_raw",
    path="data/raw/*/*.parquet",  # hourly parts
    timestamp_field="timestamp",
)

WEATHER_RAW = ParquetFileSource(
    name="weather_raw",
    path="data/raw/weather_*.parquet",
    timestamp_field="timestamp",
)

ride = Entity(name="ride_id", join_keys=["ride_id"])

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
    source=ParquetFileSource(
        name="queue_weather_source",
        path="data/raw",  # we join in the materialise step
        timestamp_field="timestamp",
    ),
)
