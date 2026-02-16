from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="weather_pipeline_extended",
    start_date=datetime(2026, 2, 14),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["weather", "air_quality", "etl"],
) as dag:

    ingest_weather = BashOperator(
        task_id="ingest_raw_weather",
        bash_command=(
            "docker exec -i weather_spark python3 -m src.jobs.ingest_hourly "
            "--lat 24.7136 --lon 46.6753 --timezone UTC"
        ),
    )

    ingest_air_quality = BashOperator(
        task_id="ingest_raw_air_quality",
        bash_command=(
            "docker exec -i weather_spark python3 -m src.jobs.ingest_air_quality "
            "--lat 24.7136 --lon 46.6753 --timezone UTC"
        ),
    )

    transform_dw = BashOperator(
        task_id="transform_spark_to_dw",
        bash_command="docker exec -i weather_spark python3 -m src.jobs.transform_hourly_spark",
    )

    [ingest_weather, ingest_air_quality] >> transform_dw