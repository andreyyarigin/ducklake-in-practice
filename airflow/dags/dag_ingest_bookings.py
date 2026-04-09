"""
dag_ingest_bookings.py — ежедневная генерация бронирований.

Расписание: 00:15 UTC (после dag_ingest_flights в 00:30 — нет, до него не нужно,
бронирования генерируются по уже существующим рейсам за вчера).

Батч: вчерашний день целиком.
Покрывает все бронирования за прошедшие сутки — booking_date = вчера.
Количество бронирований определяется детерминировано через:
  - route_profiles (base_load_factor, seasonality_type, price_tier)
  - кривую бронирований (booking curve)
  - вместимость рейса (total_seats)
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/ducklake-in-practice")

DEFAULT_ARGS = {
    "owner": "ducklake",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "email_on_failure": False,
}


def _generate_bookings_batch(**context) -> None:
    from src.generators.booking_generator import (
        _insert_bookings,
        _insert_passengers,
        _insert_price_history,
        _load_route_profiles,
        generate_bookings_batch,
    )
    from src.generators.connection import get_ducklake_connection

    logical_date: datetime = context["logical_date"]
    batch_time = logical_date.replace(tzinfo=timezone.utc)

    # Батч строго за вчерашний день
    yesterday = (batch_time - timedelta(days=1)).date()

    conn = get_ducklake_connection()

    rows = conn.execute(
        """
        SELECT flight_id, flight_number, airline_iata,
               src_airport_iata, dst_airport_iata,
               scheduled_departure, scheduled_arrival,
               status, aircraft_type, total_seats, flight_date
        FROM flights.flights
        WHERE flight_date = ?
        """,
        [yesterday],
    ).fetchall()

    cols = [
        "flight_id", "flight_number", "airline_iata",
        "src_airport_iata", "dst_airport_iata",
        "scheduled_departure", "scheduled_arrival",
        "status", "aircraft_type", "total_seats", "flight_date",
    ]
    flights = [dict(zip(cols, r)) for r in rows]

    if not flights:
        print(f"No flights found for {yesterday}. Skipping.")
        conn.close()
        return

    # Аэропорты для расчёта дистанций
    airports_rows = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports_by_iata = {
        r[0]: {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_rows if r[0]
    }

    # Профили маршрутов (load_factor, price_tier, seasonality)
    route_profiles = _load_route_profiles(conn)

    bookings, passengers, price_records = generate_bookings_batch(
        flights, airports_by_iata, route_profiles, batch_time=batch_time
    )

    _insert_passengers(conn, passengers)
    _insert_bookings(conn, bookings)
    _insert_price_history(conn, price_records)
    conn.close()

    print(
        f"Batch {yesterday}: "
        f"{len(bookings)} bookings, "
        f"{len(passengers)} new passengers, "
        f"{len(price_records)} price snapshots"
    )


def _log_batch_stats(**context) -> None:
    """Логирует агрегированную статистику за вчерашний день."""
    from src.generators.connection import get_ducklake_connection

    logical_date: datetime = context["logical_date"]
    yesterday = (logical_date - timedelta(days=1)).date()

    conn = get_ducklake_connection()

    stats = conn.execute(
        """
        SELECT
            COUNT(*) AS total_bookings,
            COUNT(DISTINCT passenger_id) AS unique_passengers,
            SUM(price_rub) AS total_revenue,
            AVG(price_rub) AS avg_price,
            COUNT(CASE WHEN status = 'confirmed' THEN 1 END) AS confirmed,
            COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled
        FROM flights.bookings
        WHERE booking_date::date = ?
        """,
        [yesterday],
    ).fetchone()

    conn.close()

    if stats:
        print(
            f"Stats [{yesterday}]: "
            f"bookings={stats[0]}, passengers={stats[1]}, "
            f"revenue={stats[2]:,.0f} RUB, avg={stats[3]:,.0f} RUB, "
            f"confirmed={stats[4]}, cancelled={stats[5]}"
        )


with DAG(
    dag_id="ingest_bookings",
    description="Ежедневная генерация бронирований за прошедший день (батч, 01:15 UTC)",
    schedule_interval="15 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "bookings"],
    max_active_runs=1,
) as dag:

    generate_bookings = PythonOperator(
        task_id="generate_bookings_batch",
        python_callable=_generate_bookings_batch,
    )

    log_stats = PythonOperator(
        task_id="log_batch_stats",
        python_callable=_log_batch_stats,
    )

    generate_bookings >> log_stats
