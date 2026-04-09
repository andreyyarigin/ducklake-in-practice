"""
routers/routes.py — эндпоинты по маршрутам.

GET /routes/top              — топ-N маршрутов по выручке за период
GET /routes/{key}/daily      — дневные метрики маршрута
GET /routes/{key}/weekly     — недельные метрики маршрута
GET /routes                  — список всех маршрутов
"""
from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.api.database import DuckLakePool, get_pool

router = APIRouter(prefix="/routes", tags=["routes"])


# ─── Модели ответа ────────────────────────────────────────────────────────────

class RouteInfo(BaseModel):
    route_key: str
    route_name: str
    src_airport_iata: str
    dst_airport_iata: str


class RouteDailyMetrics(BaseModel):
    route_key: str
    route_name: str
    flight_date: date
    total_flights: int
    cancelled_flights: int
    delayed_flights: int
    avg_delay_min: float | None
    total_capacity: int
    total_bookings: int
    total_revenue: float
    avg_ticket_price: float
    unique_passengers: int
    load_factor_pct: float


class RouteTopItem(BaseModel):
    route_key: str
    route_name: str
    total_revenue: float
    total_bookings: int
    total_flights: int
    avg_load_factor_pct: float


# ─── Эндпоинты ───────────────────────────────────────────────────────────────

@router.get("/top", response_model=list[RouteTopItem])
def get_top_routes(
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    date_from: date | None = None,
    date_to: date | None = None,
    pool: DuckLakePool = Depends(get_pool),
) -> list[RouteTopItem]:
    """Топ маршрутов по суммарной выручке за период."""
    date_from = date_from or date(2025, 1, 1)
    date_to = date_to or date.today()

    sql = """
        select
            route_key,
            route_name,
            sum(total_revenue)                                          as total_revenue,
            sum(total_bookings)                                         as total_bookings,
            sum(total_flights)                                          as total_flights,
            round(avg(load_factor_pct), 1)                             as avg_load_factor_pct
        from flights.mart_route_daily
        where flight_date between ? and ?
        group by route_key, route_name
        order by total_revenue desc
        limit ?
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [date_from, date_to, limit]).fetchall()

    return [
        RouteTopItem(
            route_key=r[0], route_name=r[1],
            total_revenue=float(r[2] or 0), total_bookings=int(r[3] or 0),
            total_flights=int(r[4] or 0), avg_load_factor_pct=float(r[5] or 0),
        )
        for r in rows
    ]


@router.get("", response_model=list[RouteInfo])
def list_routes(pool: DuckLakePool = Depends(get_pool)) -> list[RouteInfo]:
    """Список всех уникальных маршрутов."""
    sql = """
        select distinct
            route_key,
            route_name,
            src_airport_iata,
            dst_airport_iata
        from flights.mart_route_daily
        order by route_key
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql).fetchall()

    return [
        RouteInfo(
            route_key=r[0], route_name=r[1],
            src_airport_iata=r[2], dst_airport_iata=r[3],
        )
        for r in rows
    ]


@router.get("/{route_key}/daily", response_model=list[RouteDailyMetrics])
def get_route_daily(
    route_key: str,
    date_from: date | None = None,
    date_to: date | None = None,
    pool: DuckLakePool = Depends(get_pool),
) -> list[RouteDailyMetrics]:
    """Дневные метрики конкретного маршрута."""
    date_from = date_from or date(2025, 1, 1)
    date_to = date_to or date.today()

    sql = """
        select
            route_key, route_name,
            flight_date, total_flights, cancelled_flights, delayed_flights,
            avg_delay_min, total_capacity, total_bookings, total_revenue,
            avg_ticket_price, unique_passengers, load_factor_pct
        from flights.mart_route_daily
        where route_key = ?
          and flight_date between ? and ?
        order by flight_date
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [route_key, date_from, date_to]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Route '{route_key}' not found")

    return [
        RouteDailyMetrics(
            route_key=r[0], route_name=r[1],
            flight_date=r[2], total_flights=int(r[3] or 0),
            cancelled_flights=int(r[4] or 0), delayed_flights=int(r[5] or 0),
            avg_delay_min=float(r[6]) if r[6] is not None else None,
            total_capacity=int(r[7] or 0), total_bookings=int(r[8] or 0),
            total_revenue=float(r[9] or 0), avg_ticket_price=float(r[10] or 0),
            unique_passengers=int(r[11] or 0), load_factor_pct=float(r[12] or 0),
        )
        for r in rows
    ]


@router.get("/{route_key}/weekly", response_model=list[dict])
def get_route_weekly(
    route_key: str,
    date_from: date | None = None,
    date_to: date | None = None,
    pool: DuckLakePool = Depends(get_pool),
) -> list[dict]:
    """Недельные метрики конкретного маршрута."""
    date_from = date_from or date(2025, 1, 1)
    date_to = date_to or date.today()

    sql = """
        select
            route_key, route_name,
            week_start, total_flights, cancelled_flights,
            total_bookings, total_revenue, avg_ticket_price,
            total_passengers, load_factor_pct, cancellation_rate_pct
        from flights.mart_route_weekly
        where route_key = ?
          and week_start between ? and ?
        order by week_start
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [route_key, date_from, date_to]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Route '{route_key}' not found")

    cols = [
        "route_key", "route_name", "week_start",
        "total_flights", "cancelled_flights", "total_bookings",
        "total_revenue", "avg_ticket_price", "total_passengers",
        "load_factor_pct", "cancellation_rate_pct",
    ]
    return [dict(zip(cols, r)) for r in rows]
