"""
booking_generator.py — генератор бронирований.

## Логика адекватного количества бронирований

Количество бронирований на рейс определяется детерминировано:

    target_bookings(flight) =
        total_seats
        × base_load_factor(маршрут)     -- из route_profiles seed
        × season_multiplier(date)        -- сезонность
        × booking_curve(days_before)     -- кривая: когда покупают билеты

Кривая бронирований (реалистичная для РФ):
    60+ дней: 30% мест (early birds, скидки)
    30–59:    25% мест
    14–29:    20% мест
    7–13:     15% мест
    1–6:      8%  мест
    0:        2%  мест (last minute)

Один батч обрабатывает рейсы, у которых days_before попадает
в ответственный диапазон ЭТОГО батча (4 батча/сутки).
"""
from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.generators.config import (
    BOOKING_SOURCE_WEIGHTS,
    BOOKING_SOURCES,
    DUPLICATE_BOOKING_RATE,
    FARE_CLASS_WEIGHTS,
    FARE_CLASSES,
    PRICE_SEASON_MULTIPLIER_BY_MONTH,
    is_new_year_period,
    is_summer,
)
from src.generators.connection import get_ducklake_connection
from src.generators.passenger_generator import generate_passenger
from src.generators.price_generator import calculate_price, generate_price_history
from src.generators.utils import haversine_km as _haversine_km


# ─── Кривая бронирований ─────────────────────────────────────────────────────
# Доля всех бронирований по рейсу, совершаемых за N дней до вылета.
# Сумма = 1.0. Данные: типичное поведение пассажиров внутренних рейсов РФ.

BOOKING_CURVE: list[tuple[int, int, float]] = [
    # (days_from, days_to, fraction_of_total)
    (60, 999, 0.30),   # ранние покупки
    (30,  59, 0.25),   # за месяц
    (14,  29, 0.20),   # за 2 недели
    (7,   13, 0.15),   # за неделю
    (1,    6, 0.08),   # за несколько дней
    (0,    0, 0.02),   # в день вылета
]

# Ценовой tier → множитель базовой цены
PRICE_TIER_MULT: dict[str, float] = {
    "budget":  0.75,
    "medium":  1.00,
    "premium": 1.45,
}

# Тип сезонности → месяцы с повышенным спросом и их множитель
SEASONALITY_MULT: dict[str, dict[int, float]] = {
    "low": {},  # нет сезонных пиков
    "high_summer": {6: 1.40, 7: 1.55, 8: 1.45},  # летние курорты
}


def _booking_curve_fraction(days_before: int) -> float:
    """Доля бронирований, приходящаяся на день N до вылета."""
    for d_from, d_to, frac in BOOKING_CURVE:
        if d_from <= days_before <= d_to:
            span = d_to - d_from + 1
            return frac / span  # равномерно внутри диапазона
    return 0.0


def _load_factor(
    base_load: float,
    flight_date: date,
    seasonality_type: str,
) -> float:
    """Итоговый коэффициент загрузки с учётом сезонности."""
    month = flight_date.month
    season_mult = SEASONALITY_MULT.get(seasonality_type, {}).get(month, 1.0)
    if is_new_year_period(flight_date):
        season_mult = max(season_mult, 1.50)
    elif is_summer(flight_date) and seasonality_type == "low":
        season_mult = max(season_mult, 1.10)
    return min(base_load * season_mult, 0.98)  # cap 98%


def _generate_seat(total_seats: int) -> str | None:
    """Место назначается только при check-in (~60%)."""
    if random.random() > 0.60:
        return None
    rows = max(1, total_seats // 6)
    row = random.randint(1, rows)
    seat = random.choice(["A", "B", "C", "D", "E", "F"])
    return f"{row}{seat}"


def _booking_status_for_flight(flight_status: str, days_before: int) -> str:
    """Статус бронирования определяется статусом рейса и горизонтом."""
    if flight_status == "cancelled":
        return "cancelled"
    if flight_status == "arrived":
        return random.choices(
            ["no_show", "cancelled", "checked_in", "boarded", "confirmed"],
            weights=[0.02, 0.10, 0.05, 0.75, 0.08],
        )[0]
    if flight_status in ("boarding", "departed"):
        return random.choices(
            ["checked_in", "boarded", "confirmed"],
            weights=[0.2, 0.6, 0.2],
        )[0]
    # Рейс в будущем
    if days_before == 0:
        return random.choices(["checked_in", "confirmed"], weights=[0.7, 0.3])[0]
    return "confirmed"


# ─── Основной генератор ───────────────────────────────────────────────────────

def generate_bookings_batch(
    flights: list[dict[str, Any]],
    airports_by_iata: dict[str, dict[str, Any]],
    route_profiles: dict[tuple[str, str], dict[str, Any]],
    batch_time: datetime | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Сгенерировать бронирования для активных рейсов.

    Логика:
    1. Для каждого рейса определяем сколько мест должно быть забронировано
       к моменту batch_time (исходя из кривой бронирований и профиля маршрута).
    2. Сравниваем с уже существующим числом бронирований (не загружаем,
       используем оценку на основе days_before).
    3. Генерируем delta — новые бронирования этого батча.

    Returns:
        (bookings, passengers, price_history)
    """
    if batch_time is None:
        batch_time = datetime.now(timezone.utc)

    if not flights:
        return [], [], []

    bookings: list[dict] = []
    new_passengers: list[dict] = []
    price_records: list[dict] = []
    passenger_pool: list[str] = []

    for flight in flights:
        flight_id = flight["flight_id"]
        departure: datetime = flight["scheduled_departure"]
        if not hasattr(departure, "date"):
            departure = datetime.fromisoformat(str(departure))

        flight_date: date = flight["flight_date"]
        if isinstance(flight_date, str):
            flight_date = date.fromisoformat(flight_date)

        days_before = max(0, (departure.date() - batch_time.date()).days)

        # Профиль маршрута
        route_key = (flight["src_airport_iata"], flight["dst_airport_iata"])
        profile = route_profiles.get(route_key)
        base_load = profile["base_load_factor"] if profile else 0.65
        price_tier = profile["price_tier"] if profile else "medium"
        seasonality = profile["seasonality_type"] if profile else "low"

        # Итоговый load factor
        lf = _load_factor(base_load, flight_date, seasonality)

        # Сколько мест "должно быть" забронировано к сейчас
        total_seats = flight["total_seats"]
        target_booked = total_seats * lf

        # Доля, приходящаяся на текущий горизонт (за этот батч)
        # Батч запускается 1 раз в сутки → берём полную дневную долю
        daily_fraction = _booking_curve_fraction(days_before)
        batch_fraction = daily_fraction  # 1 батч в сутки

        n_new = max(0, round(target_booked * batch_fraction))
        if n_new == 0:
            continue

        # Дистанция для ценообразования
        src_ap = airports_by_iata.get(flight["src_airport_iata"])
        dst_ap = airports_by_iata.get(flight["dst_airport_iata"])
        distance_km = 1000.0
        if src_ap and dst_ap:
            distance_km = _haversine_km(
                src_ap["latitude"], src_ap["longitude"],
                dst_ap["latitude"], dst_ap["longitude"],
            )

        price_tier_mult = PRICE_TIER_MULT.get(price_tier, 1.0)

        for _ in range(n_new):
            fare_class = random.choices(FARE_CLASSES, weights=FARE_CLASS_WEIGHTS)[0]

            price = calculate_price(
                fare_class, distance_km, days_before,
                departure, flight["dst_airport_iata"],
            ) * price_tier_mult

            # Пассажир
            if passenger_pool and random.random() < 0.15:
                passenger_id = random.choice(passenger_pool)
            else:
                passenger = generate_passenger(created_at=batch_time)
                passenger_id = passenger["passenger_id"]
                new_passengers.append(passenger)
                passenger_pool.append(passenger_id)

            seat = _generate_seat(total_seats)
            status = _booking_status_for_flight(flight["status"], days_before)

            booking = {
                "booking_id": str(uuid.uuid4()),
                "flight_id": flight_id,
                "passenger_id": passenger_id,
                "booking_date": batch_time,
                "fare_class": fare_class,
                "price_rub": round(price, 2),
                "status": status,
                "seat_number": seat,
                "booking_source": random.choices(BOOKING_SOURCES, weights=BOOKING_SOURCE_WEIGHTS)[0],
                "created_at": batch_time,
                "updated_at": batch_time,
            }

            # Намеренная неидеальность: дубликат бронирования (~0.1%)
            if random.random() < DUPLICATE_BOOKING_RATE and bookings:
                dup = bookings[-1].copy()
                dup["booking_id"] = str(uuid.uuid4())
                dup["created_at"] = batch_time
                dup["updated_at"] = batch_time
                bookings.append(dup)

            bookings.append(booking)

        # Ценовые снэпшоты — только для рейсов с новыми бронированиями
        price_records.extend(
            generate_price_history(
                flight_id, departure,
                flight["dst_airport_iata"], distance_km,
                now=batch_time,
            )
        )

    return bookings, new_passengers, price_records


# ─── Вспомогательные функции для DAG ─────────────────────────────────────────

def _load_active_flights(conn, batch_time: datetime) -> list[dict]:
    """Рейсы с датой в диапазоне [вчера, +7 дней], статус не отменён/прибыл."""
    date_from = (batch_time - timedelta(days=1)).date()
    date_to = (batch_time + timedelta(days=7)).date()

    rows = conn.execute(
        """
        SELECT flight_id, flight_number, airline_iata,
               src_airport_iata, dst_airport_iata,
               scheduled_departure, scheduled_arrival,
               status, aircraft_type, total_seats, flight_date
        FROM flights.flights
        WHERE flight_date BETWEEN ? AND ?
          AND status NOT IN ('cancelled', 'arrived')
        """,
        [date_from, date_to],
    ).fetchall()

    cols = [
        "flight_id", "flight_number", "airline_iata",
        "src_airport_iata", "dst_airport_iata",
        "scheduled_departure", "scheduled_arrival",
        "status", "aircraft_type", "total_seats", "flight_date",
    ]
    return [dict(zip(cols, r)) for r in rows]


def _load_airports(conn) -> dict[str, dict]:
    rows = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    return {r[0]: {"iata_code": r[0], "latitude": r[1], "longitude": r[2]} for r in rows if r[0]}


def _load_route_profiles(conn) -> dict[tuple[str, str], dict]:
    """Загрузить профили маршрутов из DuckLake."""
    try:
        rows = conn.execute(
            """
            SELECT src_iata, dst_iata, base_load_factor,
                   price_tier, seasonality_type, competition_level
            FROM flights.route_profiles
            """
        ).fetchall()
        return {
            (r[0], r[1]): {
                "base_load_factor": r[2],
                "price_tier": r[3],
                "seasonality_type": r[4],
                "competition_level": r[5],
            }
            for r in rows
        }
    except Exception:
        # Таблица ещё не загружена — используем дефолты
        return {}


def _insert_passengers(conn, passengers: list[dict]) -> None:
    if not passengers:
        return
    conn.execute("""
        CREATE TEMP TABLE _tmp_passengers (
            passenger_id VARCHAR, first_name VARCHAR, last_name VARCHAR,
            email VARCHAR, phone VARCHAR, date_of_birth DATE,
            frequent_flyer_id VARCHAR, created_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_passengers VALUES (?,?,?,?,?,?,?,?)",
        [
            (
                p["passenger_id"], p["first_name"], p["last_name"],
                p["email"], p["phone"], p["date_of_birth"],
                p["frequent_flyer_id"], p["created_at"],
            )
            for p in passengers
        ],
    )
    conn.execute("INSERT INTO flights.passengers SELECT * FROM _tmp_passengers")
    conn.execute("DROP TABLE _tmp_passengers")


def _insert_bookings(conn, bookings: list[dict]) -> None:
    if not bookings:
        return
    conn.execute("""
        CREATE TEMP TABLE _tmp_bookings (
            booking_id VARCHAR, flight_id VARCHAR, passenger_id VARCHAR,
            booking_date TIMESTAMP, fare_class VARCHAR, price_rub DECIMAL(10,2),
            status VARCHAR, seat_number VARCHAR, booking_source VARCHAR,
            created_at TIMESTAMP, updated_at TIMESTAMP
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_bookings VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                b["booking_id"], b["flight_id"], b["passenger_id"],
                b["booking_date"], b["fare_class"], b["price_rub"],
                b["status"], b["seat_number"], b["booking_source"],
                b["created_at"], b["updated_at"],
            )
            for b in bookings
        ],
    )
    conn.execute("INSERT INTO flights.bookings SELECT * FROM _tmp_bookings")
    conn.execute("DROP TABLE _tmp_bookings")


def _insert_price_history(conn, records: list[dict]) -> None:
    if not records:
        return
    conn.execute("""
        CREATE TEMP TABLE _tmp_price_history (
            price_id VARCHAR, flight_id VARCHAR, fare_class VARCHAR,
            price_rub DECIMAL(10,2), recorded_at TIMESTAMP, days_before_departure INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_price_history VALUES (?,?,?,?,?,?)",
        [
            (
                r["price_id"], r["flight_id"], r["fare_class"],
                r["price_rub"], r["recorded_at"], r["days_before_departure"],
            )
            for r in records
        ],
    )
    conn.execute("INSERT INTO flights.price_history SELECT * FROM _tmp_price_history")
    conn.execute("DROP TABLE _tmp_price_history")


# ─── CLI точка входа ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate bookings batch")
    parser.add_argument(
        "--hour",
        default=None,
        help="Batch datetime ISO (e.g. 2026-03-15T14). Default: now.",
    )
    args = parser.parse_args()

    if args.hour:
        batch_time = datetime.fromisoformat(args.hour).replace(tzinfo=timezone.utc)
    else:
        batch_time = datetime.now(timezone.utc)

    conn = get_ducklake_connection()
    flights = _load_active_flights(conn, batch_time)

    if not flights:
        print(f"No active flights around {batch_time.date()}. Run flight_generator first.")
        conn.close()
        return

    airports = _load_airports(conn)
    route_profiles = _load_route_profiles(conn)

    bookings, passengers, price_records = generate_bookings_batch(
        flights, airports, route_profiles, batch_time
    )

    _insert_passengers(conn, passengers)
    _insert_bookings(conn, bookings)
    _insert_price_history(conn, price_records)
    conn.close()

    print(
        f"Generated: {len(bookings)} bookings, "
        f"{len(passengers)} new passengers, "
        f"{len(price_records)} price snapshots "
        f"for {len(flights)} active flights"
    )


if __name__ == "__main__":
    main()
