# Weather Data Pipeline (Open-Meteo → Spark → PostgreSQL → Airflow → Power BI)

An end-to-end data engineering project that ingests **free weather + air quality data** from **Open-Meteo** (no API key), stores raw JSON in PostgreSQL, transforms it with **Apache Spark**, loads a **star schema** into a data warehouse (`dw`), orchestrates runs with **Apache Airflow**, and powers a **Power BI dashboard**.

---

## What this project demonstrates

- **API ingestion** with Python (requests) + robust persistence (raw JSON stored as-is)
- **Incremental ETL** using an `etl_state` table (process only new raw payloads)
- **Spark transformations**: JSON parsing, explosion to hourly/daily grain, deduping, staging loads
- **Warehouse modeling**: `dim_location`, `dim_time`, `fact_weather_hourly`, `fact_weather_daily`, `fact_air_quality_hourly`
- **Orchestration** with Airflow DAG: ingest → transform/merge
- **BI readiness**: clean `dw` views for Power BI + dashboard measures

---

## Tech stack

- Python 3.x
- PostgreSQL 16 (Docker)
- Apache Spark 3.5 (Docker)
- Apache Airflow 2.9.x (Docker)
- pgAdmin (Docker)
- Power BI Desktop

---

## Architecture overview

**Flow**
1. **Ingest** (Python)  
   Calls Open-Meteo Forecast + Air Quality endpoints and inserts raw JSON into:
   - `raw.open_meteo_hourly`
   - `raw.open_meteo_air_quality_hourly`

2. **Transform** (Spark)  
   Reads only new raw payloads since last run, parses JSON arrays into rows, writes to `stg.*`

3. **Merge** (SQL)  
   `sql/03_staging_and_merge.sql` merges staging into `dw.*` and truncates staging.

4. **Orchestrate** (Airflow)  
   DAG runs hourly:
   - `ingest_raw_open_meteo`
   - `transform_spark_to_dw`

5. **Consume** (Power BI)  
   Connect to Postgres, load `dw.v_*` views, build dashboard.



![Weather Data Pipeline Architecture](https://github.com/adelsultann/weather-pipeline/edit/main/screenshot.png)
---

## Repository structure

```text
weather-pipeline/
├─ docker/
│  ├─ docker-compose.yml
│  └─ airflow/                  # airflow configs (if any)
├─ sql/
│  ├─ 01_schema.sql             # raw + dw schemas/tables
│  ├─ 02_views.sql              # BI views (v_weather_hourly, v_weather_daily, v_air_quality_hourly)
│  └─ 03_staging_and_merge.sql  # stg tables + MERGE into dw + TRUNCATE staging
├─ src/
│  ├─ config.py                 # Postgres config loader
│  ├─ extract/
│  │  ├─ open_meteo.py          # Open-Meteo fetch functions
│  │  └─ open_meteo_air_quality.py
│  ├─ load/
│  │  └─ postgres_raw.py        # insert raw JSON payloads
│  └─ jobs/
│     ├─ ingest_hourly.py       # ingest forecast into raw schema
│     ├─ ingest_air_quality.py  # ingest AQ into raw schema (if separate)
│     └─ transform_hourly_spark.py  # spark transform + merge
├─ dags/
│  └─ weather_pipeline_dag.py   # Airflow DAG definition
├─ jars/
│  └─ postgresql.jar            # JDBC driver (mounted into spark)
├─ .env.example
├─ requirements.txt
└─ README.md



