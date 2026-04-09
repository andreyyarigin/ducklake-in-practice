"""
backfill_bookings.py — полное заполнение бронирований для исторических рейсов.

Для рейсов в прошлом генерирует итоговое количество бронирований исходя из
реального load factor маршрута (без применения кривой — рейс уже состоялся).

Использование:
    python /opt/ducklake-in-practice/scripts/backfill_bookings.py
    python /opt/ducklake-in-practice/scripts/backfill_bookings.py --clear
"""
from __future__ import annotations

import argparse
import math
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.booking_generator import (
    BOOKING_SOURCES, BOOKING_SOURCE_WEIGHTS,
    FARE_CLASSES, FARE_CLASS_WEIGHTS,
    PRICE_TIER_MULT,
    _booking_status_for_flight,
    _generate_seat,
    _haversine_km,
    _load_factor,
)
from src.generators.config import DUPLICATE_BOOKING_RATE
from src.generators.connection import get_ducklake_connection
from src.generators.passenger_generator import generate_passenger
from src.generators.price_generator import calculate_price, generate_price_history

BATCH_INSERT_SIZE = 500  # строк на один INSERT SELECT


def _fmt(v: Any) -> str:
    """Форматировать значение для SQL VALUES."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (datetime, date)):
        return f"'{v}'"
    # str — экранируем одинарные кавычки
    return "'" + str(v).replace("'", "''") + "'"


def _insert_batch(conn, table: str, rows: list[tuple], columns: list[str]) -> None:
    """Вставка через VALUES в temp table + INSERT SELECT (паттерн DuckLake)."""
    if not rows:
        return

    col_defs = {
        "flights.bookings": """(
            booking_id VARCHAR, flight_id VARCHAR, passenger_id VARCHAR,
            booking_date TIMESTAMP, fare_class VARCHAR, price_rub DECIMAL(10,2),
            status VARCHAR, seat_number VARCHAR, booking_source VARCHAR,
            created_at TIMESTAMP, updated_at TIMESTAMP
        )""",
        "flights.passengers": """(
            passenger_id VARCHAR, first_name VARCHAR, last_name VARCHAR,
            email VARCHAR, phone VARCHAR, date_of_birth DATE,
            frequent_flyer_id VARCHAR, created_at TIMESTAMP
        )""",
        "flights.price_history": """(
            price_id VARCHAR, flight_id VARCHAR, fare_class VARCHAR,
            price_rub DECIMAL(10,2), recorded_at TIMESTAMP, days_before_departure INTEGER
        )""",
    }

    tmp = f"_tmp_bf_{table.split('.')[1]}"
    conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {tmp} {col_defs[table]}")
    conn.execute(f"DELETE FROM {tmp}")

    # Разбиваем на чанки по 500 строк чтобы не перегружать SQL
    chunk_size = 500
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        values_sql = ",\n".join(
            "(" + ", ".join(_fmt(v) for v in row) + ")"
            for row in chunk
        )
        conn.execute(f"INSERT INTO {tmp} VALUES {values_sql}")

    conn.execute(f"INSERT INTO {table} SELECT * FROM {tmp}")


def backfill_flight(
    flight: dict[str, Any],
    profile: dict[str, Any] | None,
    airports_by_iata: dict[str, dict],
    now: datetime,
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """Сгенерировать все бронирования для одного завершённого рейса."""
    flight_id = flight["flight_id"]
    total_seats = flight["total_seats"]
    flight_date = flight["flight_date"]
    if isinstance(flight_date, str):
        flight_date = date.fromisoformat(flight_date)

    departure = flight["scheduled_departure"]
    if not hasattr(departure, "date"):
        departure = datetime.fromisoformat(str(departure))
    if departure.tzinfo is None:
        departure = departure.replace(tzinfo=timezone.utc)

    base_load = profile["base_load_factor"] if profile else 0.65
    price_tier = profile["price_tier"] if profile else "medium"
    seasonality = profile["seasonality_type"] if profile else "low"

    lf = _load_factor(base_load, flight_date, seasonality)
    n_bookings = max(1, round(total_seats * lf))

    # Дистанция
    src_ap = airports_by_iata.get(flight["src_airport_iata"])
    dst_ap = airports_by_iata.get(flight["dst_airport_iata"])
    distance_km = 1000.0
    if src_ap and dst_ap and src_ap["latitude"] and dst_ap["latitude"]:
        distance_km = _haversine_km(
            src_ap["latitude"], src_ap["longitude"],
            dst_ap["latitude"], dst_ap["longitude"],
        )

    price_tier_mult = PRICE_TIER_MULT.get(price_tier, 1.0)

    bookings: list[tuple] = []
    new_passengers: list[tuple] = []
    price_records: list[tuple] = []
    passenger_pool: list[str] = []

    for i in range(n_bookings):
        # Бронирование происходило в разное время до вылета (по кривой)
        days_before = _pick_days_before()
        booking_dt = departure - timedelta(days=days_before)
        if booking_dt.tzinfo is None:
            booking_dt = booking_dt.replace(tzinfo=timezone.utc)
        if booking_dt > now:
            booking_dt = departure - timedelta(hours=random.randint(1, 48))
            if booking_dt.tzinfo is None:
                booking_dt = booking_dt.replace(tzinfo=timezone.utc)

        fare_class = random.choices(FARE_CLASSES, weights=FARE_CLASS_WEIGHTS)[0]
        price = calculate_price(
            fare_class, distance_km, days_before,
            departure, flight["dst_airport_iata"],
        ) * price_tier_mult

        # Пассажир (15% — повторные)
        if passenger_pool and random.random() < 0.15:
            passenger_id = random.choice(passenger_pool)
        else:
            p = generate_passenger(created_at=booking_dt)
            passenger_id = p["passenger_id"]
            new_passengers.append((
                p["passenger_id"], p["first_name"], p["last_name"],
                p.get("email"), p.get("phone"), p.get("date_of_birth"),
                p.get("frequent_flyer_id"), p["created_at"],
            ))
            passenger_pool.append(passenger_id)

        status = _booking_status_for_flight(flight["status"], 0)  # рейс в прошлом
        seat = _generate_seat(total_seats)

        bookings.append((
            str(uuid.uuid4()), flight_id, passenger_id,
            booking_dt, fare_class, round(price, 2),
            status, seat,
            random.choices(BOOKING_SOURCES, weights=BOOKING_SOURCE_WEIGHTS)[0],
            booking_dt, booking_dt,
        ))

        # Дубликат ~1%
        if random.random() < DUPLICATE_BOOKING_RATE and bookings:
            dup = list(bookings[-1])
            dup[0] = str(uuid.uuid4())
            bookings.append(tuple(dup))

    # Ценовые снэпшоты для рейса
    ph = generate_price_history(
        flight_id, departure,
        flight["dst_airport_iata"], distance_km,
        now=now,
    )
    for rec in ph:
        price_records.append((
            rec["price_id"], rec["flight_id"], rec["fare_class"],
            rec["price_rub"], rec["recorded_at"], rec["days_before_departure"],
        ))

    return bookings, new_passengers, price_records


def _pick_days_before() -> int:
    """Выбрать случайный момент до вылета согласно кривой бронирований."""
    # BOOKING_CURVE: [(60,999,0.30), (30,59,0.25), (14,29,0.20), (7,13,0.15), (1,6,0.08), (0,0,0.02)]
    ranges = [(60, 90), (30, 59), (14, 29), (7, 13), (1, 6), (0, 0)]
    weights = [0.30, 0.25, 0.20, 0.15, 0.08, 0.02]
    chosen = random.choices(ranges, weights=weights)[0]
    return random.randint(chosen[0], chosen[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill bookings for historical flights")
    parser.add_argument("--clear", action="store_true", help="Clear existing bookings/passengers/price_history first")
    parser.add_argument("--batch-size", type=int, default=200, help="Flights per commit batch")
    args = parser.parse_args()

    conn = get_ducklake_connection()
    now = datetime.now(timezone.utc)

    if args.clear:
        print("Clearing existing bookings, passengers, price_history...")
        conn.execute("DELETE FROM flights.bookings")
        conn.execute("DELETE FROM flights.passengers")
        conn.execute("DELETE FROM flights.price_history")
        print("Cleared.")

    # Только прошедшие рейсы (не будущие)
    today = now.date()
    rows = conn.execute("""
        SELECT flight_id, flight_number, airline_iata,
               src_airport_iata, dst_airport_iata,
               scheduled_departure, scheduled_arrival,
               status, aircraft_type, total_seats, flight_date
        FROM flights.flights
        WHERE flight_date < ?
        ORDER BY flight_date
    """, [today]).fetchall()

    cols = [
        "flight_id", "flight_number", "airline_iata",
        "src_airport_iata", "dst_airport_iata",
        "scheduled_departure", "scheduled_arrival",
        "status", "aircraft_type", "total_seats", "flight_date",
    ]
    flights = [dict(zip(cols, r)) for r in rows]
    print(f"Historical flights to backfill: {len(flights)}")

    # Аэропорты
    airports_rows = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports_by_iata = {
        r[0]: {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_rows if r[0]
    }

    # Профили маршрутов
    prof_rows = conn.execute("""
        SELECT src_iata, dst_iata, base_load_factor, price_tier, seasonality_type, competition_level
        FROM flights.route_profiles
    """).fetchall()
    route_profiles = {
        (r[0], r[1]): {
            "base_load_factor": r[2], "price_tier": r[3],
            "seasonality_type": r[4], "competition_level": r[5],
        }
        for r in prof_rows
    }

    total_bookings = 0
    total_passengers = 0
    total_price_records = 0

    batch_bookings: list[tuple] = []
    batch_passengers: list[tuple] = []
    batch_prices: list[tuple] = []

    for i, flight in enumerate(flights):
        route_key = (flight["src_airport_iata"], flight["dst_airport_iata"])
        profile = route_profiles.get(route_key)

        bkgs, paxs, prices = backfill_flight(flight, profile, airports_by_iata, now)
        batch_bookings.extend(bkgs)
        batch_passengers.extend(paxs)
        batch_prices.extend(prices)

        if (i + 1) % args.batch_size == 0 or i == len(flights) - 1:
            _insert_batch(conn, "flights.passengers", batch_passengers, [])
            _insert_batch(conn, "flights.bookings", batch_bookings, [])
            _insert_batch(conn, "flights.price_history", batch_prices, [])
            total_bookings += len(batch_bookings)
            total_passengers += len(batch_passengers)
            total_price_records += len(batch_prices)
            batch_bookings.clear()
            batch_passengers.clear()
            batch_prices.clear()
            print(f"  [{i+1}/{len(flights)}] bookings so far: {total_bookings:,}")

    conn.close()

    print()
    print(f"Backfill complete:")
    print(f"  Bookings      : {total_bookings:,}")
    print(f"  Passengers    : {total_passengers:,}")
    print(f"  Price records : {total_price_records:,}")
    avg_lf = total_bookings / max(1, sum(f["total_seats"] for f in flights)) * 100
    print(f"  Avg LF approx : {avg_lf:.1f}%")


if __name__ == "__main__":
    main()
