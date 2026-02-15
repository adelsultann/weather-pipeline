from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 2, 14),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["weather", "etl"],
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_open_meteo",
        bash_command=(
            "docker exec -i weather_spark python3 -m src.jobs.ingest_hourly "
            "--lat 24.7136 --lon 46.6753 --timezone UTC"
        ),
    )

    transform_dw = BashOperator(
        task_id="transform_spark_to_dw",
        bash_command="docker exec -i weather_spark python3 -m src.jobs.transform_hourly_spark",
    )

    ingest_raw >> transform_dw