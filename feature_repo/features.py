from datetime import timedelta
from pathlib import Path
import glob

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32

# ── build an explicit list of ride-wait parquet files ─────────────
ROOT = Path(__file__).resolve().parent / ".." / "data" / "raw"
queue_files = glob.glob(str(ROOT / "*" / "park=*/*.parquet"))

# ── File sources ──────────────────────────────────────────────────
QUEUE_RAW = FileSource(
    name="queue_raw",
    path=queue_files,               # ← list, not a wildcard string
    timestamp_field="timestamp",
)

WEATHER_RAW = FileSource(
    name="weather_raw",
    path="../data/weather/weather_*.parquet",
    timestamp_field="timestamp",
)

# ── entity & feature views ───────────────────────────────────────
ride = Entity(name="ride_id", join_keys=["ride_id"])

queue_hourly = FeatureView(
    name="queue_hourly",
    entities=[ride],
    ttl=None,
    schema=[Field("posted_wait", Int32)],
    online=False,
    source=QUEUE_RAW,
)

weather_hourly = FeatureView(
    name="weather_hourly",
    entities=[],            # global features
    ttl=None,
    schema=[
        Field("temp_f",      Float32),
        Field("precip_prob", Int32),
    ],
    online=False,
    source=WEATHER_RAW,
)
