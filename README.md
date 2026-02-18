````markdown
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

![Weather Dashboard](https://raw.githubusercontent.com/adelsultann/weather-pipeline/main/report_power_bi/assets/Screenshot.png)

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
````

---

## Data model (DW)

### Dimensions

* `dw.dim_location(location_id, latitude, longitude, timezone)`
* `dw.dim_time(time_id, ts_utc, date, hour, day_of_week, month, year)`

### Facts

* `dw.fact_weather_hourly(location_id, time_id, temperature_c, precipitation_mm, windspeed_kmh, weathercode, humidity_pct, pressure_hpa, visibility_m, uv_index, precip_probability_pct, fetched_at_utc)`
* `dw.fact_weather_daily(location_id, date, temp_max_c, temp_min_c, precip_sum_mm, weathercode, uv_index_max, sunrise_local, sunset_local, fetched_at_utc)`
* `dw.fact_air_quality_hourly(location_id, time_id, european_aqi, us_aqi, pm10, pm2_5, no2, o3, so2, co, fetched_at_utc)`

### BI Views

* `dw.v_weather_hourly`
* `dw.v_weather_daily`
* `dw.v_air_quality_hourly`

---

## Setup

### 1) Prerequisites

* Docker Desktop (or Docker Engine + Compose)
* Git
* (Optional) Python venv if you run anything locally
* Power BI Desktop

### 2) Environment variables

Copy and edit:

```bash
cp .env.example .env
```

Example `.env` 

```env
POSTGRES_HOST=weather_postgres
POSTGRES_PORT=5432
POSTGRES_DB=weather_dw
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=weather_pass

# Default city (Riyadh)
DEFAULT_LAT=24.7136
DEFAULT_LON=46.6753
DEFAULT_TIMEZONE=UTC
```

> Note: Inside Docker containers, `POSTGRES_HOST` should be the service name (e.g. `weather_postgres`).

---

## Run with Docker

### 1) Start all services

From the project root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Confirm containers:

```bash
docker ps
```

You should see containers like:

* `weather_postgres`
* `weather_pgadmin`
* `weather_spark` (+ worker if used)
* `airflow_webserver`, `airflow_scheduler`, `airflow_db`

### 2) Apply SQL schema + views

Run all SQL files:

```bash
docker exec -i weather_postgres psql -U weather_user -d weather_dw < sql/01_schema.sql
docker exec -i weather_postgres psql -U weather_user -d weather_dw < sql/03_staging_and_merge.sql
docker exec -i weather_postgres psql -U weather_user -d weather_dw < sql/02_views.sql
```

---

## Manual runs (without Airflow)

### 1) Ingest raw forecast (Riyadh)

```bash
docker exec -it weather_spark python3 -m src.jobs.ingest_hourly --lat 24.7136 --lon 46.6753 --timezone UTC
```

### 2) Transform + merge into DW

```bash
docker exec -it weather_spark python3 -m src.jobs.transform_hourly_spark
```

### 3) Validate loads

Raw count:

```bash
docker exec -it weather_postgres psql -U weather_user -d weather_dw -c \
"SELECT COUNT(*) AS raw_rows, MAX(fetched_at_utc) AS latest_fetch FROM raw.open_meteo_hourly;"
```

Hourly fact count:

```bash
docker exec -it weather_postgres psql -U weather_user -d weather_dw -c \
"SELECT COUNT(*) AS fact_rows FROM dw.fact_weather_hourly;"
```

Staging should be empty after merge:

```bash
docker exec -it weather_postgres psql -U weather_user -d weather_dw -c "
SELECT
  (SELECT COUNT(*) FROM stg.dim_location) AS stg_loc,
  (SELECT COUNT(*) FROM stg.dim_time) AS stg_time,
  (SELECT COUNT(*) FROM stg.fact_weather_hourly) AS stg_wh,
  (SELECT COUNT(*) FROM stg.fact_weather_daily) AS stg_wd,
  (SELECT COUNT(*) FROM stg.fact_air_quality_hourly) AS stg_aq;
"
```

---

## Airflow

### 1) Open Airflow UI

* URL: `http://localhost:8080`

### 2) DAG

* DAG name: `weather_pipeline`

Tasks:

* `ingest_raw_open_meteo`
* `transform_spark_to_dw`

### 3) Trigger a run

From terminal:

```bash
docker exec -it airflow_webserver airflow dags trigger weather_pipeline
```

---

## Power BI

### 1) Connect to PostgreSQL

Power BI Desktop → **Get Data** → PostgreSQL

* Server: `localhost`
* Database: `weather_dw`
* Credentials: `weather_user / weather_pass`

### 2) Load views (recommended)

Load these from schema `dw`:

* `v_weather_hourly`
* `v_weather_daily`
* `v_air_quality_hourly`

### 3) “Current value” measures (reliable TOPN pattern)

Example: Current temperature:

```DAX
Current Temp (C) =
VAR OneRow =
    TOPN(
        1,
        'dw v_weather_hourly',
        'dw v_weather_hourly'[ts_utc], DESC,
        'dw v_weather_hourly'[fetched_at_utc], DESC
    )
RETURN
    MAXX(OneRow, 'dw v_weather_hourly'[temperature_c])
```

AQI (EU) Now:

```DAX
AQI (EU) Now =
VAR OneRow =
    TOPN(
        1,
        'dw v_air_quality_hourly',
        'dw v_air_quality_hourly'[ts_utc], DESC,
        'dw v_air_quality_hourly'[fetched_at_utc], DESC
    )
RETURN
    MAXX(OneRow, 'dw v_air_quality_hourly'[european_aqi])
```

---

## Notes on incremental logic

The pipeline tracks last processed raw payload in:

* `dw.etl_state(pipeline_name, last_raw_id, updated_at)`

Spark reads only:

```sql
WHERE id > last_raw_id
```

Then updates `dw.etl_state` after successful staging write.

---

## Common troubleshooting

### Airflow task fails with `ModuleNotFoundError`

Your Airflow task runs a command inside `weather_spark`. Ensure:

* the code is mounted into the Spark container at the expected path
* you run: `python3 -m src.jobs.<job_name>` (not `python`)

### Spark can’t find PostgreSQL JDBC driver

Ensure the jar exists in the container:

```bash
docker exec -it weather_spark ls -lah /opt/app/jars/postgresql.jar
```

And Spark config points to it.

### Staging accumulates rows

Use **overwrite** mode when Spark writes to staging. Staging is a batch buffer, not an append-only store.

### Power BI measures show blank

If data exists, the issue is usually timestamp equality filtering. Use `TOPN(1, ...)` patterns instead of `ts_utc = MAX(ts_utc)`.

---

## Roadmap / Next improvements

* Multi-city support (`dim_city` table + slicer in Power BI)
* Backfill historical data by day range (Open-Meteo archive endpoints)
* Data quality checks (null checks, range validation, anomaly detection)
* CI for lint/tests + Docker health checks
* Store weather icons mapping in DW instead of Power BI

---

## Credits

* Data source: Open-Meteo (Forecast + Air Quality)

```


```
