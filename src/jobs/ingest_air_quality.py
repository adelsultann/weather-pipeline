# src/jobs/ingest_air_quality.py
from __future__ import annotations
import argparse
from src.config import get_postgres_config
from src.extract.air_quality import fetch_air_quality
from src.load.postgres_raw import insert_raw_payload

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lat", type=float, required=True)
    p.add_argument("--lon", type=float, required=True)
    p.add_argument("--timezone", type=str, default="UTC")
    args = p.parse_args()

    payload = fetch_air_quality(args.lat, args.lon, timezone=args.timezone)
    pg = get_postgres_config()
    
    table_name = "raw.open_meteo_air_quality_hourly"
    new_id = insert_raw_payload(pg, args.lat, args.lon, args.timezone, payload, table_name=table_name)
    print(f"Inserted raw air quality payload id={new_id}")

if __name__ == "__main__":
    main()
