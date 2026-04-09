"""
backfill.py — генерация исторических данных за диапазон дат.

Полезен для первоначального наполнения DuckLake данными
перед запуском Airflow DAG-ов.

Использование:
    python /opt/ducklake-in-practice/scripts/backfill.py \
        --from 2025-01-01 --to 2025-01-31
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.booking_generator import (
    _insert_bookings,
    _insert_passengers,
    _insert_price_history,
    generate_bookings_batch,
)
from src.generators.connection import get_ducklake_connection
from src.generators.flight_generator import (
    _insert_flights,
    _load_routes_and_airports,
    generate_flights_for_date,
)


def backfill(from_date: date, to_date: date) -> None:
    print(f"Backfill: {from_date} → {to_date}")

    conn = get_ducklake_connection()
    routes, airports_by_iata = _load_routes_and_airports(conn)

    if not routes:
        print("ERROR: no routes in DuckLake. Run load_seeds.py first.")
        conn.close()
        return

    current = from_date
    while current <= to_date:
        print(f"  {current} ...", end=" ", flush=True)

        # Рейсы
        flights = generate_flights_for_date(current, routes, airports_by_iata)
        _insert_flights(conn, flights)

        # Бронирования (8 батчей на день — каждые ~3 часа)
        total_bookings = 0
        total_passengers = 0
        total_prices = 0
        for hour_offset in range(0, 24, 3):
            batch_time = datetime(
                current.year, current.month, current.day,
                hour_offset, 0, tzinfo=timezone.utc,
            )
            # Только рейсы этого дня для батча
            bookings, passengers, price_records = generate_bookings_batch(
                flights, airports_by_iata, batch_time=batch_time
            )
            _insert_passengers(conn, passengers)
            _insert_bookings(conn, bookings)
            _insert_price_history(conn, price_records)
            total_bookings += len(bookings)
            total_passengers += len(passengers)
            total_prices += len(price_records)

        print(
            f"flights={len(flights)}, bookings={total_bookings}, "
            f"passengers={total_passengers}, price_snapshots={total_prices}"
        )
        current += timedelta(days=1)

    conn.close()
    print("Backfill complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical data into DuckLake")
    parser.add_argument("--from", dest="from_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)

    if from_date > to_date:
        print("ERROR: --from must be <= --to")
        sys.exit(1)

    backfill(from_date, to_date)


if __name__ == "__main__":
    main()
