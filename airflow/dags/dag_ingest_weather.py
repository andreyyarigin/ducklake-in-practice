"""
dag_ingest_weather.py — ежедневная загрузка погодных данных из Open-Meteo.

Расписание: 01:00 UTC каждый день (после midnight UTC).

## Демонстрация DuckLake: многотабличные ACID-транзакции (преимущество #2)

DuckLake поддерживает атомарное обновление нескольких таблиц в одной транзакции.
Это невозможно в Apache Iceberg, где транзакции ограничены одной таблицей.

В этом DAG мы явно используем это преимущество:
- Обновление статусов рейсов за вчера (flights)
- Вставка погодных наблюдений за вчера (weather_observations)
...выполняются в ОДНОЙ DuckLake транзакции через BEGIN/COMMIT.

Почему это важно:
    Если рейсы обновились, но погода не загрузилась — аналитика будет неконсистентной.
    Транзакция гарантирует: либо оба обновления видны вместе, либо ни одно.
    В Iceberg такую гарантию получить невозможно без внешнего координатора.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/ducklake-in-practice")

DEFAULT_ARGS = {
    "owner": "ducklake",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
    "email_on_failure": False,
}


def _fetch_and_store_weather(**context) -> None:
    """
    Загрузить погоду из Open-Meteo и обновить статусы рейсов в одной транзакции.

    DuckLake multi-table ACID transaction:
    ┌─────────────────────────────────────────────────────────┐
    │  BEGIN (implicit — DuckLake autocommit=off)             │
    │  UPDATE flights SET status='arrived' WHERE ...          │  ← таблица 1
    │  INSERT INTO weather_observations SELECT * FROM tmp     │  ← таблица 2
    │  COMMIT                                                  │
    └─────────────────────────────────────────────────────────┘
    При сбое любой из операций — ROLLBACK. Аналитика остаётся консистентной.
    """
    from src.generators.connection import get_ducklake_connection
    from src.generators.weather_fetcher import (
        fetch_weather_for_airports,
        insert_weather_observations,
    )

    logical_date: datetime = context["logical_date"]
    yesterday = (logical_date - timedelta(days=1)).date()
    now = datetime.now(timezone.utc)

    conn = get_ducklake_connection()

    # ── 1. Загружаем список аэропортов ──────────────────────────────
    airports_raw = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports = [
        {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_raw if r[0]
    ]

    if not airports:
        conn.close()
        raise RuntimeError("No airports found. Run load_seeds.py first.")

    # ── 2. Запрашиваем погоду (вне транзакции — внешний HTTP вызов) ─
    print(f"Fetching weather for {len(airports)} airports on {yesterday}...")
    observations = fetch_weather_for_airports(airports, yesterday)
    print(f"Fetched: {len(observations)} observations")

    if not observations:
        conn.close()
        raise RuntimeError(f"No weather data fetched for {yesterday}")

    # ── 3. Многотабличная транзакция DuckLake ───────────────────────
    #
    # DuckLake преимущество #2: одна транзакция охватывает две таблицы.
    # Iceberg не поддерживает cross-table transactions — там это было бы
    # два отдельных коммита с риском частичного обновления.
    #
    print(f"Starting DuckLake multi-table transaction for {yesterday}...")

    conn.execute("BEGIN")
    try:
        # Операция 1: обновить статусы рейсов за вчера
        conn.execute(
            """
            UPDATE flights.flights
            SET
                status     = 'arrived',
                actual_arrival = scheduled_arrival + INTERVAL '10 minutes',
                updated_at = ?
            WHERE flight_date = ?
              AND status IN ('scheduled', 'departed', 'boarding')
              AND scheduled_arrival < ?
            """,
            [now, yesterday, now],
        )
        updated_flights = conn.execute(
            """
            SELECT COUNT(*) FROM flights.flights
            WHERE flight_date = ? AND status = 'arrived' AND updated_at >= ?
            """,
            [yesterday, now],
        ).fetchone()
        flights_updated = updated_flights[0] if updated_flights else 0
        print(f"  Updated {flights_updated} flight statuses")

        # Операция 2: вставить погодные наблюдения
        # (insert_weather_observations использует temp table + INSERT SELECT)
        insert_weather_observations(conn, observations)
        print(f"  Inserted {len(observations)} weather observations")

        conn.execute("COMMIT")
        print(f"Transaction committed: flights({flights_updated}) + weather({len(observations)}) — atomic.")

    except Exception as e:
        conn.execute("ROLLBACK")
        conn.close()
        raise RuntimeError(f"Transaction rolled back: {e}") from e

    conn.close()


def _backfill_weather(**context) -> None:
    """
    Загрузить погоду с даты старта проекта (2026-01-01) если данных ещё нет.
    Запускается при первом старте для заполнения истории.
    Идемпотентно: пропускает уже загруженные даты.
    """
    from src.generators.connection import get_ducklake_connection
    from src.generators.weather_fetcher import (
        fetch_weather_for_airports,
        insert_weather_observations,
    )

    logical_date: datetime = context["logical_date"]
    conn = get_ducklake_connection()

    # Узнаём какие даты уже есть
    existing = set()
    try:
        rows = conn.execute(
            "SELECT DISTINCT observation_date FROM flights.weather_observations"
        ).fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        pass

    airports_raw = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports = [
        {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_raw if r[0]
    ]

    # Проект стартует 2026-01-01 — backfill с этой даты по вчера
    project_start = date(2026, 1, 1)
    today = logical_date.date()
    yesterday = today - timedelta(days=1)

    missing_dates = [
        project_start + timedelta(days=d)
        for d in range((yesterday - project_start).days + 1)
        if (project_start + timedelta(days=d)) not in existing
    ]

    if not missing_dates:
        print("Weather history already complete — nothing to backfill.")
        conn.close()
        return

    print(f"Backfilling weather for {len(missing_dates)} missing dates...")

    for obs_date in missing_dates:
        observations = fetch_weather_for_airports(airports, obs_date, delay_seconds=0.3)
        if observations:
            conn.execute("BEGIN")
            try:
                insert_weather_observations(conn, observations)
                conn.execute("COMMIT")
                print(f"  {obs_date}: {len(observations)} airports loaded")
            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"  {obs_date}: ROLLBACK — {e}")

    conn.close()
    print("Backfill complete.")


with DAG(
    dag_id="ingest_weather",
    description="Ежедневная загрузка погоды из Open-Meteo + multi-table транзакция",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingest", "weather", "ducklake-acid"],
    max_active_runs=1,
) as dag:

    backfill = PythonOperator(
        task_id="backfill_weather_history",
        python_callable=_backfill_weather,
        doc_md="""
        Заполняет погодную историю за последние 30 дней при первом запуске.
        Идемпотентно: пропускает уже загруженные даты.
        """,
    )

    fetch_store = PythonOperator(
        task_id="fetch_and_store_weather",
        python_callable=_fetch_and_store_weather,
        doc_md="""
        ## DuckLake multi-table ACID transaction

        Загружает погоду за вчера и обновляет статусы рейсов **в одной транзакции**.

        Это ключевое преимущество DuckLake перед Apache Iceberg:
        Iceberg поддерживает транзакции только в пределах одной таблицы.
        DuckLake позволяет атомарно обновить flights + weather_observations —
        аналитика остаётся консистентной даже при сбое посередине.
        """,
    )

    backfill >> fetch_store
