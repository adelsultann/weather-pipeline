from __future__ import annotations
import json
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp, to_date, hour, dayofweek, month, year, lit, row_number, desc, posexplode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql.window import Window

from src.config import get_postgres_config

WEATHER_RAW_TABLE = "raw.open_meteo_hourly"
AIRQ_RAW_TABLE = "raw.open_meteo_air_quality_hourly"

def run_merge_sql(pg) -> None:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            with open("sql/03_staging_and_merge.sql", "r", encoding="utf-8") as f:
                cur.execute(f.read())
        conn.commit()

def get_last_raw_id(pg, pipeline_name: str) -> int:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT last_raw_id FROM dw.etl_state WHERE pipeline_name=%s", (pipeline_name,))
            res = cur.fetchone()
            return int(res[0]) if res else 0

def set_last_raw_id(pg, pipeline_name: str, new_last_id: int) -> None:
    conn_str = f"host={pg.host} port={pg.port} dbname={pg.db} user={pg.user} password={pg.password}"
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dw.etl_state
                SET last_raw_id = %s, updated_at = now()
                WHERE pipeline_name = %s
                """,
                (new_last_id, pipeline_name),
            )
        conn.commit()

def process_weather(spark, pg, jdbc_url) -> None:
    last_id = get_last_raw_id(pg, "weather_hourly")
    
    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", WEATHER_RAW_TABLE)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("id", "latitude", "longitude", "timezone", "fetched_at_utc", col("payload").cast("string").alias("payload_str"))
        .where(col("id") > lit(last_id))
    )

    if raw_df.rdd.isEmpty():
        print("No new weather rows.")
        return

    # Schemas
    hourly_schema = StructType([
        StructField("time", ArrayType(StringType())),
        StructField("temperature_2m", ArrayType(DoubleType())),
        StructField("precipitation", ArrayType(DoubleType())),
        StructField("wind_speed_10m", ArrayType(DoubleType())), # Corrected from windspeed_10m
        StructField("weathercode", ArrayType(IntegerType())),
        StructField("relative_humidity_2m", ArrayType(DoubleType())),
        StructField("surface_pressure", ArrayType(DoubleType())),
        StructField("visibility", ArrayType(DoubleType())),
        StructField("uv_index", ArrayType(DoubleType())),
        StructField("precipitation_probability", ArrayType(DoubleType())),
    ])

    daily_schema = StructType([
        StructField("time", ArrayType(StringType())),
        StructField("sunrise", ArrayType(StringType())),
        StructField("sunset", ArrayType(StringType())),
        StructField("temperature_2m_max", ArrayType(DoubleType())),
        StructField("temperature_2m_min", ArrayType(DoubleType())),
        StructField("precipitation_sum", ArrayType(DoubleType())),
        StructField("weathercode", ArrayType(IntegerType())),
        StructField("uv_index_max", ArrayType(DoubleType())),
    ])

    payload_schema = StructType([
        StructField("hourly", hourly_schema),
        StructField("daily", daily_schema),
    ])

    parsed = raw_df.withColumn("payload", from_json(col("payload_str"), payload_schema))

    # --- 1) HOURLY WEATHER ---
    exploded_h = parsed.select(
        "latitude", "longitude", "timezone", "fetched_at_utc",
        posexplode(col("payload.hourly.time")).alias("pos", "time_str"),
        col("payload.hourly.temperature_2m").alias("temps"),
        col("payload.hourly.precipitation").alias("precips"),
        col("payload.hourly.wind_speed_10m").alias("winds"),
        col("payload.hourly.weathercode").alias("codes"),
        col("payload.hourly.relative_humidity_2m").alias("hum"),
        col("payload.hourly.surface_pressure").alias("press"),
        col("payload.hourly.visibility").alias("vis"),
        col("payload.hourly.uv_index").alias("uv"),
        col("payload.hourly.precipitation_probability").alias("precip_prob"),
    )

    fact_h = exploded_h.select(
        "latitude", "longitude", "timezone",
        to_timestamp(col("time_str")).alias("ts_utc"),
        col("temps")[col("pos")].alias("temperature_c"),
        col("precips")[col("pos")].alias("precipitation_mm"),
        col("winds")[col("pos")].alias("windspeed_kmh"),
        col("codes")[col("pos")].alias("weathercode"),
        col("hum")[col("pos")].alias("humidity_pct"),
        col("press")[col("pos")].alias("pressure_hpa"),
        col("vis")[col("pos")].alias("visibility_m"),
        col("uv")[col("pos")].alias("uv_index"),
        col("precip_prob")[col("pos")].alias("precip_probability_pct"),
        col("fetched_at_utc"),
    )

    # Deduplicate
    w_h = Window.partitionBy("latitude", "longitude", "ts_utc").orderBy(desc("fetched_at_utc"))
    fact_h = fact_h.withColumn("rn", row_number().over(w_h)).where(col("rn") == 1).drop("rn")

    # --- 2) DAILY WEATHER ---
    exploded_d = parsed.select(
        "latitude", "longitude", "timezone", "fetched_at_utc",
        posexplode(col("payload.daily.time")).alias("pos", "day_str"),
        col("payload.daily.temperature_2m_max").alias("t_max"),
        col("payload.daily.temperature_2m_min").alias("t_min"),
        col("payload.daily.precipitation_sum").alias("p_sum"),
        col("payload.daily.weathercode").alias("codes"),
        col("payload.daily.uv_index_max").alias("uv_max"),
        col("payload.daily.sunrise").alias("sunrises"),
        col("payload.daily.sunset").alias("sunsets"),
    )

    fact_d = exploded_d.select(
        "latitude", "longitude", "timezone",
        to_date(col("day_str")).alias("date"),
        col("t_max")[col("pos")].alias("temp_max_c"),
        col("t_min")[col("pos")].alias("temp_min_c"),
        col("p_sum")[col("pos")].alias("precip_sum_mm"),
        col("codes")[col("pos")].alias("weathercode"),
        col("uv_max")[col("pos")].alias("uv_index_max"),
        to_timestamp(col("sunrises")[col("pos")]).alias("sunrise_local"),
        to_timestamp(col("sunsets")[col("pos")]).alias("sunset_local"),
        col("fetched_at_utc"),
    )

    # Deduplicate
    w_d = Window.partitionBy("latitude", "longitude", "date").orderBy(desc("fetched_at_utc"))
    fact_d = fact_d.withColumn("rn", row_number().over(w_d)).where(col("rn") == 1).drop("rn")

    # Dimensions
    stg_location = fact_h.select("latitude", "longitude", "timezone").distinct()
    stg_time = fact_h.select(
        col("ts_utc"),
        to_date(col("ts_utc")).alias("date"),
        hour(col("ts_utc")).cast("smallint").alias("hour"),
        dayofweek(col("ts_utc")).cast("smallint").alias("day_of_week"),
        month(col("ts_utc")).cast("smallint").alias("month"),
        year(col("ts_utc")).cast("smallint").alias("year"),
    ).distinct()

    # Writes
    stg_location.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.dim_location").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()
    stg_time.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.dim_time").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()
    fact_h.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.fact_weather_hourly").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()
    fact_d.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.fact_weather_daily").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()

    # Update state
    max_id = raw_df.agg({"id": "max"}).collect()[0][0]
    if max_id:
        set_last_raw_id(pg, "weather_hourly", max_id)

def process_air_quality(spark, pg, jdbc_url) -> None:
    last_id = get_last_raw_id(pg, "air_quality_hourly")
    
    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", AIRQ_RAW_TABLE)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("id", "latitude", "longitude", "timezone", "fetched_at_utc", col("payload").cast("string").alias("payload_str"))
        .where(col("id") > lit(last_id))
    )

    if raw_df.rdd.isEmpty():
        print("No new air quality rows.")
        return

    # Schema
    aq_hourly_schema = StructType([
        StructField("time", ArrayType(StringType())),
        StructField("european_aqi", ArrayType(DoubleType())),
        StructField("us_aqi", ArrayType(DoubleType())),
        StructField("pm10", ArrayType(DoubleType())),
        StructField("pm2_5", ArrayType(DoubleType())),
        StructField("nitrogen_dioxide", ArrayType(DoubleType())),
        StructField("ozone", ArrayType(DoubleType())),
        StructField("sulphur_dioxide", ArrayType(DoubleType())),
        StructField("carbon_monoxide", ArrayType(DoubleType())),
    ])

    payload_schema = StructType([
        StructField("hourly", aq_hourly_schema),
    ])

    parsed = raw_df.withColumn("payload", from_json(col("payload_str"), payload_schema))

    exploded = parsed.select(
        "latitude", "longitude", "timezone", "fetched_at_utc",
        posexplode(col("payload.hourly.time")).alias("pos", "time_str"),
        col("payload.hourly.european_aqi").alias("eur_aqi"),
        col("payload.hourly.us_aqi").alias("us_aqi"),
        col("payload.hourly.pm10").alias("pm10"),
        col("payload.hourly.pm2_5").alias("pm25"),
        col("payload.hourly.nitrogen_dioxide").alias("no2"),
        col("payload.hourly.ozone").alias("o3"),
        col("payload.hourly.sulphur_dioxide").alias("so2"),
        col("payload.hourly.carbon_monoxide").alias("co"),
    )

    fact = exploded.select(
        "latitude", "longitude", "timezone",
        to_timestamp(col("time_str")).alias("ts_utc"),
        col("eur_aqi")[col("pos")].alias("european_aqi"),
        col("us_aqi")[col("pos")].alias("us_aqi"),
        col("pm10")[col("pos")].alias("pm10"),
        col("pm25")[col("pos")].alias("pm2_5"),
        col("no2")[col("pos")].alias("no2"),
        col("o3")[col("pos")].alias("o3"),
        col("so2")[col("pos")].alias("so2"),
        col("co")[col("pos")].alias("co"),
        col("fetched_at_utc"),
    )

    # Deduplicate
    w = Window.partitionBy("latitude", "longitude", "ts_utc").orderBy(desc("fetched_at_utc"))
    fact = fact.withColumn("rn", row_number().over(w)).where(col("rn") == 1).drop("rn")

    # Time dimension (ensure all timestamps are in dim_time)
    stg_time = fact.select(
        col("ts_utc"),
        to_date(col("ts_utc")).alias("date"),
        hour(col("ts_utc")).cast("smallint").alias("hour"),
        dayofweek(col("ts_utc")).cast("smallint").alias("day_of_week"),
        month(col("ts_utc")).cast("smallint").alias("month"),
        year(col("ts_utc")).cast("smallint").alias("year"),
    ).distinct()

    # Writes
    stg_time.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.dim_time").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()
    fact.write.format("jdbc").option("url", jdbc_url).option("dbtable", "stg.fact_air_quality_hourly").option("user", pg.user).option("password", pg.password).option("driver", "org.postgresql.Driver").mode("overwrite").save()

    # Update state
    max_id = raw_df.agg({"id": "max"}).collect()[0][0]
    if max_id:
        set_last_raw_id(pg, "air_quality_hourly", max_id)

def main() -> None:
    pg = get_postgres_config()
    jdbc_url = f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.db}"

    spark = (
        SparkSession.builder
        .appName("weather-transform-extended")
        .config("spark.jars", "jars/postgresql.jar")
        .getOrCreate()
    )

    # Process all parts
    process_weather(spark, pg, jdbc_url)
    process_air_quality(spark, pg, jdbc_url)

    # Run MERGE SQL to move data from staging to DW
    run_merge_sql(pg)
    
    print("Spark extended transform complete + MERGE applied.")
    spark.stop()

if __name__ == "__main__":
    main()