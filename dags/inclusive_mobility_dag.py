from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from lib.acces_libre_fetcher import fetch_accessibility_data
from lib.mobility_score import compute_daily_mobility_scores
from lib.open_meteo_fetcher import fetch_weather_data
from lib.raw_to_formatted_accessibility import format_accessibility_data
from lib.raw_to_formatted_weather import format_weather_data


DEFAULT_ARGS = {
    "owner": "lab3",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="inclusive_mobility_daily_pipeline",
    description="Daily accessibility and weather risk pipeline for inclusive mobility.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 5),
    schedule_interval="@daily",
    catchup=False,
    tags=["lab3", "inclusive-mobility", "accessibility", "weather"],
) as dag:
    extract_accessibility_data = PythonOperator(
        task_id="extract_accessibility_data",
        python_callable=fetch_accessibility_data,
    )

    extract_weather_data = PythonOperator(
        task_id="extract_weather_data",
        python_callable=fetch_weather_data,
    )

    format_accessibility = PythonOperator(
        task_id="format_accessibility_data",
        python_callable=format_accessibility_data,
    )

    format_weather = PythonOperator(
        task_id="format_weather_data",
        python_callable=format_weather_data,
    )

    compute_scores = PythonOperator(
        task_id="compute_mobility_scores",
        python_callable=compute_daily_mobility_scores,
    )

    chain(
        [extract_accessibility_data, extract_weather_data],
        [format_accessibility, format_weather],
        compute_scores,
    )
