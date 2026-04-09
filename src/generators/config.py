"""
config.py — конфигурация генерации данных.

Содержит бизнес-параметры: сезонность, dynamic pricing, задержки,
список ВС, классы обслуживания и источники бронирований.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


# ─── Самолёты ────────────────────────────────────────────────────────────────

AIRCRAFT_TYPES: list[tuple[str, int]] = [
    # (тип, кол-во мест economy)
    ("A320", 150),
    ("A321", 180),
    ("B737", 162),
    ("B738", 189),
    ("SU95", 87),
    ("A319", 128),
    ("B767", 216),
    ("A330", 268),
]

# Веса для выбора типа ВС (чаще короткий/средний маршрут)
AIRCRAFT_WEIGHTS: list[float] = [0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.04, 0.03]


# ─── Статусы ─────────────────────────────────────────────────────────────────

FLIGHT_STATUSES: list[str] = [
    "scheduled", "boarding", "departed", "arrived", "cancelled", "delayed"
]

BOOKING_STATUSES: list[str] = [
    "confirmed", "cancelled", "checked_in", "boarded", "no_show"
]

FARE_CLASSES: list[str] = ["economy", "business", "first"]
FARE_CLASS_WEIGHTS: list[float] = [0.78, 0.18, 0.04]

BOOKING_SOURCES: list[str] = ["web", "mobile", "agency", "corporate"]
BOOKING_SOURCE_WEIGHTS: list[float] = [0.40, 0.35, 0.15, 0.10]


# ─── Базовые цены (рубли) ────────────────────────────────────────────────────

BASE_PRICE_RUB: dict[str, float] = {
    "economy": 5_500.0,
    "business": 18_000.0,
    "first": 45_000.0,
}

# Множители дальности: (distance_km_threshold, multiplier)
DISTANCE_PRICE_TIERS: list[tuple[int, float]] = [
    (500, 0.7),
    (1000, 1.0),
    (2000, 1.4),
    (4000, 1.8),
    (9999, 2.3),
]


# ─── Dynamic pricing: дней до вылета ─────────────────────────────────────────

DAYS_BEFORE_DEPARTURE_TIERS: list[tuple[int, float]] = [
    # (days_before, price_multiplier) — порог включительно
    (1, 2.0),
    (7, 1.5),
    (14, 1.2),
    (30, 1.0),
    (60, 0.85),
    (9999, 0.7),
]


# ─── Сезонность ──────────────────────────────────────────────────────────────

# Южные аэропорты — дополнительный коэффициент зимой
SOUTHERN_AIRPORTS: set[str] = {"AER", "KRR", "SIP", "GRV", "NAL", "MRV", "IGT"}

# Коэффициенты загрузки рейсов по месяцу (1..12)
LOAD_FACTOR_BY_MONTH: dict[int, float] = {
    1: 0.65, 2: 0.60, 3: 0.68, 4: 0.72,
    5: 0.80, 6: 0.90, 7: 0.95, 8: 0.92,
    9: 0.82, 10: 0.75, 11: 0.65, 12: 0.85,
}

PRICE_SEASON_MULTIPLIER_BY_MONTH: dict[int, float] = {
    1: 1.1, 2: 0.90, 3: 0.95, 4: 1.0,
    5: 1.1, 6: 1.3, 7: 1.4, 8: 1.35,
    9: 1.1, 10: 1.0, 11: 0.90, 12: 1.5,
}

# Новогодние праздники: 25 дек — 10 янв
NEW_YEAR_PRICE_MULT: float = 1.6
NEW_YEAR_LOAD_MULT: float = 1.5

# Лето: июнь — август
SUMMER_PRICE_MULT: float = 1.4
SUMMER_LOAD_MULT: float = 1.3


def is_new_year_period(d: date) -> bool:
    return (d.month == 12 and d.day >= 25) or (d.month == 1 and d.day <= 10)


def is_summer(d: date) -> bool:
    return d.month in (6, 7, 8)


def is_winter(d: date) -> bool:
    return d.month in (12, 1, 2)


# ─── Задержки и отмены ───────────────────────────────────────────────────────

DELAY_PROBABILITY: float = 0.15        # 15% рейсов задерживаются
CANCEL_PROBABILITY: float = 0.025      # 2.5% рейсов отменяются
DELAY_MEAN_MINUTES: float = 30.0
DELAY_STD_MINUTES: float = 45.0
WINTER_DELAY_MULT: float = 1.5         # Зимой задержки в 1.5 раза длиннее


# ─── Намеренные неидеальности данных ────────────────────────────────────────

DUPLICATE_BOOKING_RATE: float = 0.01   # 1% — дубликаты бронирований
MISSING_EMAIL_RATE: float = 0.03       # 3% — пустой email
NEGATIVE_PRICE_RATE: float = 0.005     # 0.5% — отрицательная цена
MISSING_STATUS_UPDATE_RATE: float = 0.02  # 2% — рейс не обновил статус


# ─── Количества ──────────────────────────────────────────────────────────────

@dataclass
class GeneratorConfig:
    flights_per_day: int = field(
        default_factory=lambda: int(os.environ.get("GEN_FLIGHTS_PER_DAY", "800"))
    )
    # Ценовых точек на рейс (для price_history)
    price_snapshots_per_flight: int = 8
    # Расписание генерируется на N дней вперёд
    schedule_days_ahead: int = 7


# Глобальный конфиг (singleton)
GEN_CONFIG = GeneratorConfig()
