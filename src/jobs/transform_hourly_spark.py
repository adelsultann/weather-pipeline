from __future__ import annotations
import json
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp, to_date, hour, dayofweek, month, year, lit, row_number, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql.window import Window

from src.config import get_postgres_config

RAW_TABLE = "raw.open_meteo_hourly"

def run_merge_sql(pg) -> None:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            with open("sql/03_staging_and_merge.sql", "r", encoding="utf-8") as f:
                cur.execute(f.read())
        conn.commit()


# this two function is for the incremental load
# we get the last raw id from the etl_state table and then we read the raw table from that id to the end
# and then we update the last raw id to the new last id

def get_last_raw_id(pg) -> int:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT last_raw_id FROM dw.etl_state WHERE pipeline_name='weather_hourly'")
            return int(cur.fetchone()[0])

def set_last_raw_id(pg, new_last_id: int) -> None:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dw.etl_state
                SET last_raw_id = %s, updated_at = now()
                WHERE pipeline_name='weather_hourly'
                """,
                (new_last_id,),
            )
        conn.commit()

def main() -> None:
    pg = get_postgres_config()
    jdbc_url = f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.db}"
    jdbc_props = {"user": pg.user, "password": pg.password, "driver": "org.postgresql.Driver"}

    spark = (
        SparkSession.builder
        .appName("weather-transform-hourly")
        .config("spark.jars", "jars/postgresql.jar")
        .getOrCreate()
    )

    # 1) Read raw table (payload is JSONB -> comes as string via JDBC)
    last_id = get_last_raw_id(pg)

raw_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", RAW_TABLE)
    .option("user", pg.user)
    .option("password", pg.password)
    .option("driver", "org.postgresql.Driver")
    .load()
    # select the id column to use it for the incremental load
    .select("id", "latitude", "longitude", "timezone", "fetched_at_utc", col("payload").cast("string").alias("payload_str"))
    # filter the raw table to get only the new rows
    .where(col("id") > lit(last_id))
)

if raw_df.rdd.isEmpty():
    print(f"No new raw rows to process (last_raw_id={last_id}).")
    spark.stop()
    return

    # 2) Define the subset of Open-Meteo schema we use
    hourly_schema = StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m", ArrayType(DoubleType()), True),
        StructField("precipitation", ArrayType(DoubleType()), True),
        StructField("windspeed_10m", ArrayType(DoubleType()), True),
        StructField("weathercode", ArrayType(IntegerType()), True),
    ])

    payload_schema = StructType([
        StructField("hourly", hourly_schema, True),
    ])

    parsed = raw_df.withColumn("payload", from_json(col("payload_str"), payload_schema))

    # 3) Convert hourly arrays into rows (explode by index via posexplode)
    # Spark’s clean way: posexplode time array then pick same position from other arrays
    from pyspark.sql.functions import posexplode

    exploded = parsed.select(
        "latitude", "longitude", "timezone", "fetched_at_utc",
        posexplode(col("payload.hourly.time")).alias("pos", "time_str"),
        col("payload.hourly.temperature_2m").alias("temps"),
        col("payload.hourly.precipitation").alias("precips"),
        col("payload.hourly.windspeed_10m").alias("winds"),
        col("payload.hourly.weathercode").alias("codes"),
    )

    fact = exploded.select(
        "latitude", "longitude", "timezone",
        to_timestamp(col("time_str")).alias("ts_utc"),
        col("temps")[col("pos")].alias("temperature_c"),
        col("precips")[col("pos")].alias("precipitation_mm"),
        col("winds")[col("pos")].alias("windspeed_kmh"),
        col("codes")[col("pos")].alias("weathercode"),
        col("fetched_at_utc"),
    )

    # 3.1) Deduplicate fact table to ensure one row per (location, ts_utc)
    # This prevents the Postgres MERGE error: "more than one source row matches any one target row"
    window_spec = Window.partitionBy("latitude", "longitude", "ts_utc").orderBy(desc("fetched_at_utc"))
    fact = fact.withColumn("rn", row_number().over(window_spec)).where(col("rn") == 1).drop("rn")

    # 4) Build staging dims
    stg_location = fact.select("latitude", "longitude", "timezone").distinct()

    stg_time = fact.select(
        col("ts_utc"),
        to_date(col("ts_utc")).alias("date"),
        hour(col("ts_utc")).cast("smallint").alias("hour"),
        dayofweek(col("ts_utc")).cast("smallint").alias("day_of_week"),
        month(col("ts_utc")).cast("smallint").alias("month"),
        year(col("ts_utc")).cast("smallint").alias("year"),
    ).distinct()

    # 5) Write staging tables (overwrite each run)
    (stg_location.write.format("jdbc")
     .option("url", jdbc_url).option("dbtable", "stg.dim_location")
     .option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver")
     .mode("overwrite").save())

    (stg_time.write.format("jdbc")
     .option("url", jdbc_url).option("dbtable", "stg.dim_time")
     .option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver")
     .mode("overwrite").save())

    (fact.write.format("jdbc")
     .option("url", jdbc_url).option("dbtable", "stg.fact_weather_hourly")
     .option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver")
     .mode("overwrite").save())

    spark.stop()

    # 6) Upsert into DW using Postgres MERGE
    run_merge_sql(pg)
    print("Spark transform complete + MERGE applied.")

if __name__ == "__main__":
    main()