# src/load/postgres_raw.py
from __future__ import annotations
import json
from typing import Any, Dict
import psycopg
from psycopg.rows import dict_row
from src.config import PostgresConfig

def insert_raw_payload(
    pg: PostgresConfig,
    latitude: float,
    longitude: float,
    timezone: str,
    payload: Dict[str, Any],
) -> int:

    # create connection string using f-string using the class attributes in the PostgresConfig class 
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    # row_factory=dict_row is used to return the rows as a dictionary with the column names as keys
    with psycopg.connect(conn_str, row_factory=dict_row) as conn:

        # post the payload to the database in the raw.open_meteo_hourly table
        with conn.cursor() as cur:
            print(f"Inserting raw payload for lat={latitude}, lon={longitude}...")
            cur.execute(
                """
                INSERT INTO raw.open_meteo_hourly (latitude, longitude, timezone, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                RETURNING id;
                """,
                (latitude, longitude, timezone, json.dumps(payload)),
            )
            new_id = cur.fetchone()["id"]
            conn.commit()
            return int(new_id)