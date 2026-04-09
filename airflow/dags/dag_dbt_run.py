"""
dag_dbt_run.py — оркестрация dbt через Airflow.

DAGs:
  - dbt_run_daily   : ежедневно в 02:00 UTC → staging → intermediate → marts
  - dbt_test_daily  : ежедневно в 03:30 UTC → dbt test (после maintenance)
  - dbt_docs_weekly : воскресенье 04:00 UTC → dbt docs generate

Все модели материализуются как table (не incremental/microbatch).
Полный прогон раз в сутки достаточен при объёме sandbox.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/ducklake-in-practice")

DBT_PROJECT_DIR = "/opt/ducklake-in-practice/dbt/ducklake_flights"
DBT_PROFILES_DIR = "/opt/ducklake-in-practice/dbt/ducklake_flights"

DEFAULT_ARGS = {
    "owner": "ducklake",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
    "email_on_failure": False,
}


def _run_dbt(command: list[str], **context) -> None:
    """Запустить dbt-команду в subprocess с логированием stdout/stderr."""
    import os

    env = os.environ.copy()
    env.setdefault("DUCKLAKE_PASSWORD", "ducklake_secret_change_me")
    env.setdefault("RUSTFS_ACCESS_KEY", "rustfsadmin")
    env.setdefault("RUSTFS_SECRET_KEY", "rustfsadmin123")

    full_cmd = [
        "dbt",
        *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--no-use-colors",
    ]

    print(f"Running: {' '.join(full_cmd)}")

    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        env=env,
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt exited with code {result.returncode}")


# ─── DAG: dbt_run_daily ───────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_run_daily",
    description="Ежедневный dbt run: staging → intermediate → marts (02:00 UTC)",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "transform"],
    max_active_runs=1,
) as dag_daily:

    deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=_run_dbt,
        op_args=[["deps"]],
    )

    run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=_run_dbt,
        op_args=[["run", "--select", "staging"]],
    )

    # int_bookings_daily_agg запускается отдельным шагом:
    # это тяжёлая агрегация 7.8M бронирований, требует отдельного DuckDB процесса
    run_bookings_agg = PythonOperator(
        task_id="dbt_run_bookings_agg",
        python_callable=_run_dbt,
        op_args=[["run", "--select", "int_bookings_daily_agg"]],
    )

    run_intermediate = PythonOperator(
        task_id="dbt_run_intermediate",
        python_callable=_run_dbt,
        op_args=[["run", "--select", "intermediate"]],
    )

    run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=_run_dbt,
        op_args=[["run", "--select", "marts"]],
    )

    deps >> run_staging >> run_bookings_agg >> run_intermediate >> run_marts


# ─── DAG: dbt_test_daily ──────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_test_daily",
    description="Ежедневный dbt test (03:30 UTC, после maintenance)",
    schedule_interval="30 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={
        **DEFAULT_ARGS,
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["dbt", "test"],
    max_active_runs=1,
) as dag_test:

    deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=_run_dbt,
        op_args=[["deps"]],
    )

    test_staging = PythonOperator(
        task_id="dbt_test_staging",
        python_callable=_run_dbt,
        op_args=[["test", "--select", "staging"]],
    )

    test_intermediate = PythonOperator(
        task_id="dbt_test_intermediate",
        python_callable=_run_dbt,
        op_args=[["test", "--select", "intermediate"]],
    )

    test_marts = PythonOperator(
        task_id="dbt_test_marts",
        python_callable=_run_dbt,
        op_args=[["test", "--select", "marts"]],
    )

    deps >> test_staging >> test_intermediate >> test_marts


# ─── DAG: dbt_docs_weekly ─────────────────────────────────────────────────────

with DAG(
    dag_id="dbt_docs_weekly",
    description="Еженедельная генерация dbt docs (воскресенье 04:00 UTC)",
    schedule_interval="0 4 * * 0",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "docs"],
    max_active_runs=1,
) as dag_docs:

    generate_docs = PythonOperator(
        task_id="dbt_docs_generate",
        python_callable=_run_dbt,
        op_args=[["docs", "generate"]],
    )
