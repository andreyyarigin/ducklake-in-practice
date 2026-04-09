"""
routers/airlines.py — эндпоинты по авиакомпаниям.

GET /airlines                    — список авиакомпаний
GET /airlines/stats              — агрегированная статистика за период
GET /airlines/{iata}/daily       — дневные метрики авиакомпании
"""
from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.api.database import DuckLakePool, get_pool

router = APIRouter(prefix="/airlines", tags=["airlines"])


class AirlineInfo(BaseModel):
    iata_code: str
    name: str | None
    country: str | None


class AirlineStats(BaseModel):
    airline_iata: str
    airline_name: str | None
    total_flights: int
    cancelled_flights: int
    delayed_flights: int
    avg_delay_min: float | None
    total_bookings: int
    total_revenue: float
    avg_ticket_price: float
    unique_passengers: int
    avg_load_factor_pct: float
    cancellation_rate_pct: float


class AirlineDailyMetrics(BaseModel):
    airline_iata: str
    airline_name: str | None
    flight_date: date
    total_flights: int
    cancelled_flights: int
    delayed_flights: int
    avg_delay_min: float | None
    total_capacity: int
    active_routes: int
    total_bookings: int
    total_revenue: float
    avg_ticket_price: float
    load_factor_pct: float
    cancellation_rate_pct: float


@router.get("", response_model=list[AirlineInfo])
def list_airlines(pool: DuckLakePool = Depends(get_pool)) -> list[AirlineInfo]:
    """Список всех активных авиакомпаний РФ."""
    sql = """
        select iata_code, name, country
        from flights.airlines
        where active = true
        order by name
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql).fetchall()

    return [AirlineInfo(iata_code=r[0], name=r[1], country=r[2]) for r in rows]


@router.get("/stats", response_model=list[AirlineStats])
def get_airlines_stats(
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    pool: DuckLakePool = Depends(get_pool),
) -> list[AirlineStats]:
    """Агрегированная статистика по авиакомпаниям за период, отсортированная по выручке."""
    date_from = date_from or date(2025, 1, 1)
    date_to = date_to or date.today()

    sql = """
        select
            airline_iata,
            airline_name,
            sum(total_flights)                                          as total_flights,
            sum(cancelled_flights)                                      as cancelled_flights,
            sum(delayed_flights)                                        as delayed_flights,
            round(avg(avg_delay_min), 1)                               as avg_delay_min,
            sum(total_bookings)                                         as total_bookings,
            sum(total_revenue)                                          as total_revenue,
            round(avg(avg_ticket_price), 2)                            as avg_ticket_price,
            sum(unique_passengers)                                      as unique_passengers,
            round(avg(load_factor_pct), 1)                             as avg_load_factor_pct,
            round(
                sum(cancelled_flights)::float / nullif(sum(total_flights), 0) * 100, 1
            )                                                           as cancellation_rate_pct
        from flights.mart_airline_daily
        where flight_date between ? and ?
        group by airline_iata, airline_name
        order by total_revenue desc
        limit ?
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [date_from, date_to, limit]).fetchall()

    return [
        AirlineStats(
            airline_iata=r[0], airline_name=r[1],
            total_flights=int(r[2] or 0), cancelled_flights=int(r[3] or 0),
            delayed_flights=int(r[4] or 0),
            avg_delay_min=float(r[5]) if r[5] is not None else None,
            total_bookings=int(r[6] or 0), total_revenue=float(r[7] or 0),
            avg_ticket_price=float(r[8] or 0), unique_passengers=int(r[9] or 0),
            avg_load_factor_pct=float(r[10] or 0), cancellation_rate_pct=float(r[11] or 0),
        )
        for r in rows
    ]


@router.get("/{iata}/daily", response_model=list[AirlineDailyMetrics])
def get_airline_daily(
    iata: str,
    date_from: date | None = None,
    date_to: date | None = None,
    pool: DuckLakePool = Depends(get_pool),
) -> list[AirlineDailyMetrics]:
    """Дневные метрики конкретной авиакомпании."""
    date_from = date_from or date(2025, 1, 1)
    date_to = date_to or date.today()

    sql = """
        select
            airline_iata, airline_name, flight_date,
            total_flights, cancelled_flights, delayed_flights,
            avg_delay_min, total_capacity, active_routes,
            total_bookings, total_revenue, avg_ticket_price,
            load_factor_pct, cancellation_rate_pct
        from flights.mart_airline_daily
        where upper(airline_iata) = upper(?)
          and flight_date between ? and ?
        order by flight_date
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [iata, date_from, date_to]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Airline '{iata}' not found")

    return [
        AirlineDailyMetrics(
            airline_iata=r[0], airline_name=r[1], flight_date=r[2],
            total_flights=int(r[3] or 0), cancelled_flights=int(r[4] or 0),
            delayed_flights=int(r[5] or 0),
            avg_delay_min=float(r[6]) if r[6] is not None else None,
            total_capacity=int(r[7] or 0), active_routes=int(r[8] or 0),
            total_bookings=int(r[9] or 0), total_revenue=float(r[10] or 0),
            avg_ticket_price=float(r[11] or 0),
            load_factor_pct=float(r[12] or 0), cancellation_rate_pct=float(r[13] or 0),
        )
        for r in rows
    ]
