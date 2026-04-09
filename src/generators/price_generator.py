"""
price_generator.py — генератор истории цен.

Для каждого рейса создаёт несколько ценовых снэпшотов,
отражающих dynamic pricing (чем ближе к вылету, тем дороже).
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from src.generators.config import (
    BASE_PRICE_RUB,
    DAYS_BEFORE_DEPARTURE_TIERS,
    DISTANCE_PRICE_TIERS,
    FARE_CLASSES,
    GEN_CONFIG,
    NEGATIVE_PRICE_RATE,
    PRICE_SEASON_MULTIPLIER_BY_MONTH,
    SOUTHERN_AIRPORTS,
    is_new_year_period,
    is_summer,
    is_winter,
)


def _distance_multiplier(distance_km: float) -> float:
    for threshold, mult in DISTANCE_PRICE_TIERS:
        if distance_km <= threshold:
            return mult
    return DISTANCE_PRICE_TIERS[-1][1]


def _days_before_multiplier(days_before: int) -> float:
    for threshold, mult in DAYS_BEFORE_DEPARTURE_TIERS:
        if days_before <= threshold:
            return mult
    return DAYS_BEFORE_DEPARTURE_TIERS[-1][1]


def _season_multiplier(departure: datetime, dst_iata: str) -> float:
    d = departure.date()
    mult = PRICE_SEASON_MULTIPLIER_BY_MONTH[d.month]

    # Южные направления зимой — дополнительный коэффициент
    if dst_iata in SOUTHERN_AIRPORTS and is_winter(d):
        mult *= 1.3

    return mult


def calculate_price(
    fare_class: str,
    distance_km: float,
    days_before: int,
    departure: datetime,
    dst_iata: str,
) -> float:
    """Вычислить цену с учётом всех коэффициентов."""
    base = BASE_PRICE_RUB[fare_class]
    price = (
        base
        * _distance_multiplier(distance_km)
        * _days_before_multiplier(days_before)
        * _season_multiplier(departure, dst_iata)
    )
    # Небольшой случайный шум ±5%
    price *= random.uniform(0.95, 1.05)
    return round(price, 2)


def generate_price_history(
    flight_id: str,
    departure: datetime,
    dst_iata: str,
    distance_km: float,
    now: datetime | None = None,
) -> list[dict]:
    """
    Сгенерировать ценовые снэпшоты для рейса.

    Создаёт price_snapshots_per_flight записей на рейс по каждому классу.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if departure.tzinfo is None:
        departure = departure.replace(tzinfo=timezone.utc)
    days_total = (departure - now).days
    n_snapshots = GEN_CONFIG.price_snapshots_per_flight

    # Точки: равномерно от 60 дней до 0 дней до вылета, но только в прошлом
    max_days_back = min(60, max(days_total + 1, 0))
    if max_days_back == 0:
        snapshot_days = [0]
    else:
        step = max_days_back / n_snapshots
        snapshot_days = [int(max_days_back - i * step) for i in range(n_snapshots)]
        snapshot_days = sorted(set(max(0, d) for d in snapshot_days), reverse=True)

    records: list[dict] = []
    for days_before in snapshot_days:
        recorded_at = departure - __import__("datetime").timedelta(days=days_before)
        # Не записываем точки в будущем
        if recorded_at > now:
            continue

        for fare_class in FARE_CLASSES:
            price = calculate_price(fare_class, distance_km, days_before, departure, dst_iata)

            # Намеренная неидеальность: 0.5% — отрицательная цена
            if random.random() < NEGATIVE_PRICE_RATE:
                price = -abs(price)

            records.append({
                "price_id": str(uuid.uuid4()),
                "flight_id": flight_id,
                "fare_class": fare_class,
                "price_rub": price,
                "recorded_at": recorded_at,
                "days_before_departure": days_before,
            })

    return records
