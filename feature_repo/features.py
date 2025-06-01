from feast import Entity, FeatureView, Field, FileSource
from feast.file_format import ParquetFormat
from feast.types import Float32, Int32
from datetime import timedelta

# ✅ Hive partitioning only for ride-wait data
queue_format = ParquetFormat(hive_partitioning=True)

QUEUE_RAW = FileSource(
    name="queue_raw",
    # match only the ride-wait parquet *files*
    path="../data/raw/*.parquet/park=*/*.parquet",
    timestamp_field="timestamp",
    file_format=queue_format,
)

# ✅ Weather files are *not* hive-partitioned, so no ParquetFormat here
WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/raw/weather_*.parquet",
    timestamp_field="timestamp",
)

ride = Entity(name="ride_id", join_keys=["ride_id"])

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
