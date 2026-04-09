"""
schema.py — DDL для создания таблиц DuckLake.

Выполняется один раз при инициализации (load_seeds.py).
Примечание: DuckLake не поддерживает PRIMARY KEY/UNIQUE constraints —
уникальность обеспечивается на уровне генераторов (UUID).
"""
from __future__ import annotations

import duckdb

SCHEMA_STATEMENTS: list[str] = [
    # ── Seed tables ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS flights.airports (
        airport_id      INTEGER,
        name            VARCHAR NOT NULL,
        city            VARCHAR,
        country         VARCHAR,
        iata_code       VARCHAR(3),
        icao_code       VARCHAR(4),
        latitude        DOUBLE,
        longitude       DOUBLE,
        altitude        INTEGER,
        timezone_offset DOUBLE,
        timezone_name   VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flights.airlines (
        airline_id  INTEGER,
        name        VARCHAR NOT NULL,
        iata_code   VARCHAR(2),
        icao_code   VARCHAR(3),
        country     VARCHAR,
        active      BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flights.routes (
        airline_iata      VARCHAR(2),
        src_airport_iata  VARCHAR(3),
        dst_airport_iata  VARCHAR(3),
        codeshare         BOOLEAN,
        stops             INTEGER,
        equipment         VARCHAR
    )
    """,
    # ── Transactional tables ─────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS flights.passengers (
        passenger_id      VARCHAR,
        first_name        VARCHAR NOT NULL,
        last_name         VARCHAR NOT NULL,
        email             VARCHAR,
        phone             VARCHAR,
        date_of_birth     DATE,
        frequent_flyer_id VARCHAR,
        created_at        TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flights.flights (
        flight_id            VARCHAR,
        flight_number        VARCHAR NOT NULL,
        airline_iata         VARCHAR(2),
        src_airport_iata     VARCHAR(3),
        dst_airport_iata     VARCHAR(3),
        scheduled_departure  TIMESTAMP NOT NULL,
        scheduled_arrival    TIMESTAMP NOT NULL,
        actual_departure     TIMESTAMP,
        actual_arrival       TIMESTAMP,
        status               VARCHAR NOT NULL,
        aircraft_type        VARCHAR,
        total_seats          INTEGER,
        flight_date          DATE NOT NULL,
        created_at           TIMESTAMP NOT NULL,
        updated_at           TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flights.bookings (
        booking_id      VARCHAR,
        flight_id       VARCHAR NOT NULL,
        passenger_id    VARCHAR NOT NULL,
        booking_date    TIMESTAMP NOT NULL,
        fare_class      VARCHAR NOT NULL,
        price_rub       DECIMAL(10, 2) NOT NULL,
        status          VARCHAR NOT NULL,
        seat_number     VARCHAR,
        booking_source  VARCHAR,
        created_at      TIMESTAMP NOT NULL,
        updated_at      TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flights.price_history (
        price_id              VARCHAR,
        flight_id             VARCHAR NOT NULL,
        fare_class            VARCHAR NOT NULL,
        price_rub             DECIMAL(10, 2) NOT NULL,
        recorded_at           TIMESTAMP NOT NULL,
        days_before_departure INTEGER NOT NULL
    )
    """,
    # ── Seed: профили маршрутов ──────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS flights.route_profiles (
        src_iata          VARCHAR(3) NOT NULL,
        dst_iata          VARCHAR(3) NOT NULL,
        base_load_factor  DOUBLE NOT NULL,
        price_tier        VARCHAR NOT NULL,
        seasonality_type  VARCHAR NOT NULL,
        competition_level VARCHAR NOT NULL,
        notes             VARCHAR
    )
    """,
    # ── Seed: характеристики воздушных судов ─────────────────────────
    """
    CREATE TABLE IF NOT EXISTS flights.aircraft_types (
        icao_code            VARCHAR(4) NOT NULL,
        iata_code            VARCHAR(3),
        manufacturer         VARCHAR NOT NULL,
        model                VARCHAR NOT NULL,
        family               VARCHAR,
        seats_economy        INTEGER,
        seats_business       INTEGER,
        seats_first          INTEGER,
        seats_total          INTEGER,
        range_km             INTEGER,
        fuel_burn_kg_per_km  DOUBLE,
        first_flight_year    INTEGER,
        engine_type          VARCHAR,
        body_type            VARCHAR
    )
    """,
    # ── Transactional: наблюдения погоды по аэропортам ───────────────
    """
    CREATE TABLE IF NOT EXISTS flights.weather_observations (
        observation_id        VARCHAR NOT NULL,
        airport_iata          VARCHAR(3) NOT NULL,
        observation_date      DATE NOT NULL,
        temperature_min_c     DOUBLE,
        temperature_max_c     DOUBLE,
        temperature_mean_c    DOUBLE,
        precipitation_mm      DOUBLE,
        windspeed_max_kmh     DOUBLE,
        windgusts_max_kmh     DOUBLE,
        visibility_min_km     DOUBLE,
        snowfall_cm           DOUBLE,
        weather_code          INTEGER,
        weather_description   VARCHAR,
        fetched_at            TIMESTAMP NOT NULL
    )
    """,
]


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Создать все таблицы DuckLake, если их ещё нет."""
    for stmt in SCHEMA_STATEMENTS:
        conn.execute(stmt)
    print("Schema created / verified OK")
