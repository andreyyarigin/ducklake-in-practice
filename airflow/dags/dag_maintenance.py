"""
dag_maintenance.py — обслуживание DuckLake.

Расписание: ежедневно в 03:00 UTC.

Задачи:
  1. expire_snapshots   — удалить снэпшоты старше 7 дней
  2. compact_tables     — компактификация Parquet-файлов (мелкие → крупные)
  3. vacuum_catalog     — очистка orphaned файлов из PG-каталога
  4. stats_report       — вывод статистики таблиц DuckLake
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
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

SNAPSHOT_RETENTION_DAYS = 7

# Таблицы, подлежащие компактификации (большой объём инкрементальных вставок)
COMPACT_TABLES = [
    "flights.flights",
    "flights.bookings",
    "flights.passengers",
    "flights.price_history",
]


def _expire_snapshots(**context) -> None:
    """Удалить снэпшоты DuckLake старше SNAPSHOT_RETENTION_DAYS дней."""
    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()
    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)

    # DuckLake: EXPIRE SNAPSHOTS удаляет старые версии данных
    # Передаём cutoff как строку без timezone-суффикса — DuckLake принимает TIMESTAMP
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        f"CALL ducklake_expire_snapshots('flights', TIMESTAMP '{cutoff_str}')"
    )

    conn.close()
    print(f"Expired snapshots older than {cutoff.date()} (retention={SNAPSHOT_RETENTION_DAYS}d)")


def _compact_tables(**context) -> None:
    """Компактифицировать Parquet-файлы в DuckLake (мелкие файлы → крупные)."""
    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()
    results = []

    for table in COMPACT_TABLES:
        try:
            conn.execute(f"CALL ducklake_compact('{table}')")
            results.append(f"  {table}: OK")
        except Exception as e:
            # Компактификация — best-effort, не прерываем DAG
            results.append(f"  {table}: SKIP ({e})")

    conn.close()
    print("Compaction results:\n" + "\n".join(results))


def _vacuum_catalog(**context) -> None:
    """Удалить orphaned файлы, на которые нет ссылок из каталога."""
    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()

    try:
        conn.execute("CALL ducklake_vacuum('flights')")
        print("Vacuum completed OK")
    except Exception as e:
        print(f"Vacuum skipped: {e}")
    finally:
        conn.close()


def _stats_report(**context) -> None:
    """Вывести статистику таблиц: кол-во строк, размер, последняя запись."""
    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()

    tables = [
        ("flights.airports", "airport_id", None),
        ("flights.airlines", "airline_id", None),
        ("flights.routes", "airline_iata", None),
        ("flights.flights", "flight_id", "created_at"),
        ("flights.bookings", "booking_id", "created_at"),
        ("flights.passengers", "passenger_id", "created_at"),
        ("flights.price_history", "price_id", "recorded_at"),
    ]

    lines = [f"{'Table':<35} {'Rows':>10} {'Last record':<25}"]
    lines.append("-" * 75)

    for table, pk_col, ts_col in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if ts_col:
                last = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()[0]
                last_str = str(last)[:19] if last else "—"
            else:
                last_str = "static"
            lines.append(f"{table:<35} {count:>10,} {last_str:<25}")
        except Exception as e:
            lines.append(f"{table:<35} {'ERROR':>10} {str(e)[:25]:<25}")

    conn.close()
    print("\n".join(lines))


def _check_data_freshness(**context) -> None:
    """
    Проверить свежесть данных: если последнее бронирование старше 3 часов — warning.
    В prod здесь была бы отправка алерта (Slack / email).
    """
    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()
    now = datetime.now(timezone.utc)

    last_booking = conn.execute(
        "SELECT MAX(created_at) FROM flights.bookings"
    ).fetchone()[0]

    conn.close()

    if last_booking is None:
        print("WARNING: No bookings in DuckLake yet.")
        return

    # Приводим к offset-aware если нужно
    if hasattr(last_booking, "tzinfo") and last_booking.tzinfo is None:
        last_booking = last_booking.replace(tzinfo=timezone.utc)

    lag = now - last_booking
    if lag > timedelta(hours=3):
        print(
            f"WARNING: Data freshness alert! "
            f"Last booking: {last_booking.isoformat()}, lag={lag}"
        )
    else:
        print(f"Data freshness OK. Last booking: {last_booking.isoformat()}, lag={lag}")


with DAG(
    dag_id="maintenance",
    description="Ежедневное обслуживание DuckLake: снэпшоты, компактификация, vacuum",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
    max_active_runs=1,
) as dag:

    expire_snapshots = PythonOperator(
        task_id="expire_snapshots",
        python_callable=_expire_snapshots,
    )

    compact_tables = PythonOperator(
        task_id="compact_tables",
        python_callable=_compact_tables,
    )

    vacuum_catalog = PythonOperator(
        task_id="vacuum_catalog",
        python_callable=_vacuum_catalog,
    )

    stats_report = PythonOperator(
        task_id="stats_report",
        python_callable=_stats_report,
    )

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=_check_data_freshness,
    )

    # expire → compact → vacuum → stats + freshness (параллельно в конце)
    expire_snapshots >> compact_tables >> vacuum_catalog >> [stats_report, check_freshness]
