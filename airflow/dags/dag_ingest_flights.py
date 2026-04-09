"""
dag_ingest_flights.py — ежедневная генерация рейсов.

Расписание: 00:30 UTC каждый день.
Генерирует рейсы на следующие 7 дней вперёд + обновляет статусы вчерашних.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/ducklake-in-practice")

DEFAULT_ARGS = {
    "owner": "ducklake",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
}


def _generate_flights_task(flight_date_str: str, **context) -> None:
    from src.generators.connection import get_ducklake_connection
    from src.generators.flight_generator import (
        _insert_flights,
        _load_routes_and_airports,
        generate_flights_for_date,
    )

    flight_date = date.fromisoformat(flight_date_str)
    conn = get_ducklake_connection()

    routes, airports_by_iata = _load_routes_and_airports(conn)
    if not routes:
        raise RuntimeError("No routes found in DuckLake. Run load_seeds.py first.")

    flights = generate_flights_for_date(flight_date, routes, airports_by_iata)
    _insert_flights(conn, flights)
    conn.close()

    print(f"Generated {len(flights)} flights for {flight_date}")


def _generate_schedule(**context) -> None:
    """Генерирует рейсы на 7 дней вперёд начиная с сегодня."""
    logical_date: datetime = context["logical_date"]
    today = logical_date.date()

    from src.generators.config import GEN_CONFIG

    for offset in range(GEN_CONFIG.schedule_days_ahead):
        target_date = today + timedelta(days=offset)
        _generate_flights_task(target_date.isoformat())

    print(f"Schedule generated: {today} + {GEN_CONFIG.schedule_days_ahead} days")


def _update_flight_statuses(**context) -> None:
    """Обновляет статусы рейсов за вчера (departed → arrived и т.д.)."""
    import duckdb

    logical_date: datetime = context["logical_date"]
    yesterday = (logical_date - timedelta(days=1)).date()
    now = datetime.now(timezone.utc)

    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()

    # Рейсы вчерашнего дня, которые должны были прилететь
    conn.execute(
        """
        UPDATE flights.flights
        SET
            status = 'arrived',
            actual_arrival = scheduled_arrival + INTERVAL '10 minutes',
            updated_at = ?
        WHERE flight_date = ?
          AND status IN ('scheduled', 'departed', 'boarding')
          AND scheduled_arrival < ?
        """,
        [now, yesterday, now],
    )

    updated = conn.execute(
        """
        SELECT COUNT(*) FROM flights.flights
        WHERE flight_date = ? AND status = 'arrived' AND updated_at >= ?
        """,
        [yesterday, now],
    ).fetchone()

    conn.close()
    print(f"Updated flight statuses for {yesterday}: {updated[0] if updated else 0} rows")


with DAG(
    dag_id="ingest_flights",
    description="Ежедневная генерация рейсов на 7 дней вперёд",
    schedule_interval="30 0 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "flights"],
    max_active_runs=1,
) as dag:

    generate_schedule = PythonOperator(
        task_id="generate_schedule",
        python_callable=_generate_schedule,
    )

    update_statuses = PythonOperator(
        task_id="update_flight_statuses",
        python_callable=_update_flight_statuses,
    )

    generate_schedule >> update_statuses
