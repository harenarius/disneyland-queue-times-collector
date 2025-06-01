# ingest/pull_weather.py
import os
import time
from pathlib import Path

import httpx
import pandas as pd
import typer

LAT, LON = 33.8121, -117.9190  # Disneyland Resort
API_KEY = os.environ.get("OPENWEATHER_API_KEY")
URL = (
    "https://api.openweathermap.org/data/3.0/onecall"
    f"?lat={LAT}&lon={LON}"
    "&exclude=current,minutely,daily,alerts"
    "&units=imperial"
    f"&appid={API_KEY}"
)
HEADERS = {"User-Agent": "DLQueueTimes/0.1 (contact: you@example.com)"}
DATA_DIR = Path("data/weather")
DATA_DIR.mkdir(parents=True, exist_ok=True)
app = typer.Typer(add_completion=False)


def fetch_hourly() -> pd.DataFrame:
    for attempt in range(4):
        r = httpx.get(URL, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            hours = r.json()["hourly"][:24]
            return pd.DataFrame(
                dict(
                    timestamp=[pd.Timestamp.utcfromtimestamp(h["dt"]) for h in hours],
                    temp_f=[h["temp"] for h in hours],
                    precip_prob=[int(h.get("pop", 0) * 100) for h in hours],
                )
            )
        time.sleep(2**attempt)
    r.raise_for_status()


@app.command()
def pull():
    if not API_KEY:
        raise typer.Exit("OPENWEATHER_API_KEY env-var not set")

    df = fetch_hourly()
    date_str = df["timestamp"].iloc[0].strftime("%Y-%m-%d")
    out = DATA_DIR / f"weather_{date_str}.parquet"
    df.to_parquet(out, index=False)
    typer.secho(f"✅ wrote {len(df)} rows to {out}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
