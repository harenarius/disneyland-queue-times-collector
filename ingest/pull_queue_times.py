import datetime as dt
from pathlib import Path

import httpx
import pandas as pd
import typer
from tenacity import retry, stop_after_attempt, wait_exponential

app = typer.Typer(add_completion=False)
URL = "https://queue-times.com/en-US/parks/{pid}/queue_times.json"
PARK_IDS = {"dl": 1, "dca": 2}
DATA = Path("data/raw")
DATA.mkdir(parents=True, exist_ok=True)


@retry(stop=stop_after_attempt(5), wait=wait_exponential())
def fetch(park: str):  # list[dict]
    r = httpx.get(URL.format(pid=PARK_IDS[park]), timeout=15)
    r.raise_for_status()
    return r.json()["lands"]


def flat(lands, park, ts):
    rows = []
    for land in lands:
        for ride in land["rides"]:
            rows.append(
                dict(
                    timestamp=ts,
                    park=park,
                    ride_id=ride["id"],
                    ride_name=ride["name"],
                    status=ride["is_open"],
                    posted_wait=ride["wait_time"],
                    last_update=ride["last_updated"],
                )
            )
    return rows


@app.command()
def pull(
    park: str = typer.Option(..., help="dl or dca"),
    as_of: dt.datetime | None = typer.Option(
        None, formats=["%Y-%m-%dT%H:%M"], help="override UTC timestamp"
    ),
):
    ts = as_of or dt.datetime.utcnow().replace(second=0, microsecond=0)
    typer.echo(f"Fetching {park.upper()} @ {ts.isoformat()}Z …")
    df = pd.DataFrame(flat(fetch(park), park, ts))
    out = DATA / f"{ts:%Y-%m-%d}.parquet"
    df.to_parquet(out, partition_cols=["park"], compression="snappy", index=False)
    typer.secho(f"✅ wrote {len(df):,} rows to {out}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
