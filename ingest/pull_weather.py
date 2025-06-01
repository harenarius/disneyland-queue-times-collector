from pathlib import Path

import httpx
import pandas as pd
import typer

app = typer.Typer(add_completion=False)

DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://api.weather.gov/gridpoints/LOX/107,37/forecast/hourly"
HEADERS = {"User-Agent": "DLQueueTimes/0.1 (contact: you@example.com)"}


def fetch() -> pd.DataFrame:
    r = httpx.get(URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    periods = r.json()["properties"]["periods"][:24]  # next 24 h
    return pd.DataFrame(
        dict(
            timestamp=[pd.Timestamp(p["startTime"]).tz_convert("UTC") for p in periods],
            temp_f=[p["temperature"] for p in periods],
            precip_prob=[
                p["probabilityOfPrecipitation"]["value"] or 0 for p in periods
            ],
        )
    )


@app.command()
def pull():
    df = fetch()
    date_str = df["timestamp"].iloc[0].strftime("%Y-%m-%d")
    out = DATA_DIR / f"weather_{date_str}.parquet"
    df.to_parquet(out, index=False)
    typer.secho(f"✅ wrote {len(df)} rows to {out}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
