"""
dag_export_serving_store.py — обновление serving store после dbt marts.

Запускается ежедневно в 03:00 UTC (после dbt_run_daily в 02:00).
Экспортирует mart-таблицы из DuckLake в /serving/flights.duckdb атомарно:
  1. Записывает данные в flights_new.duckdb
  2. Переименовывает в flights.duckdb (атомарная операция)

Superset читает из flights.duckdb — файл никогда не бывает в частичном состоянии.
"""
from __future__ import annotations

import subprocess
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "ducklake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
}


def _export_serving_store(**context) -> None:
    import os
    result = subprocess.run(
        ["python", "/opt/ducklake-in-practice/docker/export-serving-store.py"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"export-serving-store exited with code {result.returncode}")


with DAG(
    dag_id="export_serving_store",
    description="Экспорт marts из DuckLake в serving store (flights.duckdb) после dbt run",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["serving", "export"],
    max_active_runs=1,
) as dag:

    export = PythonOperator(
        task_id="export_serving_store",
        python_callable=_export_serving_store,
    )
