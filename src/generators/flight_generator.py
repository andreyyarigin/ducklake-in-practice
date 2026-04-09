"""
flight_generator.py — генератор рейсов.

Генерирует расписание рейсов на заданную дату на основе маршрутов
из DuckLake. Применяет бизнес-логику задержек, отмен и сезонности.

Использование:
    python -m src.generators.flight_generator --date 2025-06-15
"""
from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np

from src.generators.config import (
    AIRCRAFT_TYPES,
    AIRCRAFT_WEIGHTS,
    CANCEL_PROBABILITY,
    DELAY_MEAN_MINUTES,
    DELAY_PROBABILITY,
    DELAY_STD_MINUTES,
    GEN_CONFIG,
    MISSING_STATUS_UPDATE_RATE,
    WINTER_DELAY_MULT,
    is_winter,
)
from src.generators.connection import get_ducklake_connection
from src.generators.utils import haversine_km as _haversine_km


def _flight_duration_minutes(distance_km: float) -> int:
    """Расчётное время полёта: 800 км/ч + 30 мин наземных операций."""
    return int(distance_km / 800 * 60) + 30


def _pick_aircraft() -> tuple[str, int]:
    return random.choices(AIRCRAFT_TYPES, weights=AIRCRAFT_WEIGHTS, k=1)[0]


def _departure_hour() -> int:
    """Реалистичное распределение вылетов по часам (пики: 6-9, 13-15, 18-21)."""
    peaks = [7, 8, 9, 13, 14, 18, 19, 20]
    off_peak = list(range(6, 23))
    pool = peaks * 3 + off_peak
    return random.choice(pool)


# ─── Основной генератор ───────────────────────────────────────────────────────

def generate_flights_for_date(
    flight_date: date,
    routes: list[dict[str, Any]],
    airports_by_iata: dict[str, dict[str, Any]],
    target_count: int | None = None,
) -> list[dict[str, Any]]:
    """
    Сгенерировать рейсы для заданной даты.

    Args:
        flight_date: дата рейсов
        routes: список маршрутов из DuckLake (airline_iata, src, dst, equipment)
        airports_by_iata: словарь аэропортов по IATA-коду
        target_count: целевое кол-во рейсов (по умолчанию из GEN_CONFIG)

    Returns:
        Список словарей — одна запись на рейс.
    """
    if target_count is None:
        target_count = GEN_CONFIG.flights_per_day

    now = datetime.now(timezone.utc)

    # Выбираем случайное подмножество маршрутов
    selected_routes = random.choices(routes, k=target_count)

    flights: list[dict[str, Any]] = []
    airline_flight_counters: dict[str, int] = {}

    for route in selected_routes:
        airline = route["airline_iata"]
        src = route["src_airport_iata"]
        dst = route["dst_airport_iata"]
        equipment = route.get("equipment") or ""

        src_ap = airports_by_iata.get(src)
        dst_ap = airports_by_iata.get(dst)

        if not src_ap or not dst_ap:
            continue

        distance_km = _haversine_km(
            src_ap["latitude"], src_ap["longitude"],
            dst_ap["latitude"], dst_ap["longitude"],
        )
        duration_min = _flight_duration_minutes(distance_km)

        # Номер рейса
        counter = airline_flight_counters.get(airline, 100)
        airline_flight_counters[airline] = counter + 1
        flight_number = f"{airline}-{counter}"

        # Время вылета
        dep_hour = _departure_hour()
        dep_minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        scheduled_departure = datetime(
            flight_date.year, flight_date.month, flight_date.day,
            dep_hour, dep_minute, tzinfo=timezone.utc,
        )
        scheduled_arrival = scheduled_departure + timedelta(minutes=duration_min)

        # Тип ВС — предпочитаем из equipment маршрута
        equip_tokens = [e.strip() for e in equipment.split() if e.strip()]
        if equip_tokens:
            equip_token = random.choice(equip_tokens)
            matched = next(
                ((t, s) for t, s in AIRCRAFT_TYPES if equip_token in t or t in equip_token),
                None,
            )
            aircraft_type, total_seats = matched if matched else _pick_aircraft()
        else:
            aircraft_type, total_seats = _pick_aircraft()

        # Статус и задержки
        status = "scheduled"
        actual_departure = None
        actual_arrival = None

        flight_ts = scheduled_departure
        is_past = flight_ts < now

        if is_past:
            # Отмена
            if random.random() < CANCEL_PROBABILITY:
                status = "cancelled"
            else:
                # Задержка
                delay_mult = WINTER_DELAY_MULT if is_winter(flight_date) else 1.0
                if random.random() < DELAY_PROBABILITY:
                    delay_min = max(
                        5,
                        int(np.random.normal(
                            DELAY_MEAN_MINUTES * delay_mult,
                            DELAY_STD_MINUTES * delay_mult,
                        )),
                    )
                    status = "delayed"
                    actual_departure = scheduled_departure + timedelta(minutes=delay_min)
                else:
                    actual_departure = scheduled_departure + timedelta(minutes=random.randint(-2, 5))

                if actual_departure:
                    actual_arrival = actual_departure + timedelta(minutes=duration_min)

                # 2% — зависший статус (намеренная неидеальность)
                if random.random() < MISSING_STATUS_UPDATE_RATE:
                    status = "scheduled"
                else:
                    status = "arrived"

        flight_id = str(uuid.uuid4())
        flights.append({
            "flight_id": flight_id,
            "flight_number": flight_number,
            "airline_iata": airline,
            "src_airport_iata": src,
            "dst_airport_iata": dst,
            "scheduled_departure": scheduled_departure,
            "scheduled_arrival": scheduled_arrival,
            "actual_departure": actual_departure,
            "actual_arrival": actual_arrival,
            "status": status,
            "aircraft_type": aircraft_type,
            "total_seats": total_seats,
            "flight_date": flight_date,
            "created_at": now,
            "updated_at": now,
        })

    return flights


# ─── CLI точка входа ──────────────────────────────────────────────────────────

def _load_routes_and_airports(conn) -> tuple[list[dict], dict[str, dict]]:
    routes_raw = conn.execute(
        "SELECT airline_iata, src_airport_iata, dst_airport_iata, equipment "
        "FROM flights.routes WHERE stops = 0"
    ).fetchall()
    routes = [
        {
            "airline_iata": r[0],
            "src_airport_iata": r[1],
            "dst_airport_iata": r[2],
            "equipment": r[3],
        }
        for r in routes_raw
    ]

    airports_raw = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports_by_iata = {
        r[0]: {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_raw
        if r[0]
    }

    return routes, airports_by_iata


def _insert_flights(conn, flights: list[dict]) -> None:
    if not flights:
        return
    conn.execute("""
        CREATE TEMP TABLE _tmp_flights (
            flight_id VARCHAR, flight_number VARCHAR, airline_iata VARCHAR,
            src_airport_iata VARCHAR, dst_airport_iata VARCHAR,
            scheduled_departure TIMESTAMP, scheduled_arrival TIMESTAMP,
            actual_departure TIMESTAMP, actual_arrival TIMESTAMP,
            status VARCHAR, aircraft_type VARCHAR, total_seats INTEGER,
            flight_date DATE, created_at TIMESTAMP, updated_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_flights VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                f["flight_id"], f["flight_number"], f["airline_iata"],
                f["src_airport_iata"], f["dst_airport_iata"],
                f["scheduled_departure"], f["scheduled_arrival"],
                f["actual_departure"], f["actual_arrival"],
                f["status"], f["aircraft_type"], f["total_seats"],
                f["flight_date"], f["created_at"], f["updated_at"],
            )
            for f in flights
        ],
    )
    conn.execute("INSERT INTO flights.flights SELECT * FROM _tmp_flights")
    conn.execute("DROP TABLE _tmp_flights")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate flights for a given date")
    parser.add_argument("--date", required=True, help="Flight date YYYY-MM-DD")
    parser.add_argument("--count", type=int, default=None, help="Number of flights")
    args = parser.parse_args()

    flight_date = date.fromisoformat(args.date)

    conn = get_ducklake_connection()
    routes, airports_by_iata = _load_routes_and_airports(conn)

    if not routes:
        print("ERROR: no routes found in DuckLake. Run load_seeds.py first.")
        return

    flights = generate_flights_for_date(flight_date, routes, airports_by_iata, args.count)
    _insert_flights(conn, flights)
    conn.close()

    print(f"Generated {len(flights)} flights for {flight_date}")


if __name__ == "__main__":
    main()
