# src/jobs/ingest_hourly.py
from __future__ import annotations
import argparse
from src.config import get_postgres_config
from src.extract.open_meteo import fetch_hourly_forecast
from src.load.postgres_raw import insert_raw_payload

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lat", type=float, required=True)
    p.add_argument("--lon", type=float, required=True)
    p.add_argument("--timezone", type=str, default="UTC")
    args = p.parse_args()

    payload = fetch_hourly_forecast(args.lat, args.lon, timezone=args.timezone)
    pg = get_postgres_config()
    new_id = insert_raw_payload(pg, args.lat, args.lon, args.timezone, payload)
    print(f"Inserted raw payload id={new_id}")

if __name__ == "__main__":
    main()