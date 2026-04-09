"""
routers/time_travel.py — эндпоинты для демонстрации Time Travel DuckLake.

GET /time-travel/snapshots              — список снэпшотов DuckLake
GET /time-travel/compare                — сравнение метрик между двумя снэпшотами
GET /time-travel/price-history/{flight} — история цен на рейс по снэпшотам
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.api.database import DuckLakePool, get_pool

router = APIRouter(prefix="/time-travel", tags=["time-travel"])


class SnapshotInfo(BaseModel):
    snapshot_id: int
    created_at: datetime
    schema_version: int | None


class PricePoint(BaseModel):
    recorded_at: datetime
    fare_class: str
    price_rub: float
    days_before_departure: int


class SnapshotCompareResult(BaseModel):
    metric: str
    snapshot_a: float | None
    snapshot_b: float | None
    diff: float | None
    diff_pct: float | None


@router.get("/snapshots", response_model=list[SnapshotInfo])
def list_snapshots(
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    pool: DuckLakePool = Depends(get_pool),
) -> list[SnapshotInfo]:
    """
    Список последних снэпшотов DuckLake для таблицы flights.
    Демонстрирует возможность time travel — каждая транзакция создаёт снэпшот.
    """
    sql = """
        select snapshot_id, snapshot_time, schema_version
        from ducklake_snapshots('flights')
        order by snapshot_id desc
        limit ?
    """
    with pool.acquire() as conn:
        try:
            rows = conn.execute(sql, [limit]).fetchall()
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Cannot read DuckLake snapshots: {e}",
            )

    return [
        SnapshotInfo(
            snapshot_id=int(r[0]),
            created_at=r[1],
            schema_version=int(r[2]) if r[2] is not None else None,
        )
        for r in rows
    ]


@router.get("/compare", response_model=list[SnapshotCompareResult])
def compare_snapshots(
    snapshot_a: Annotated[int, Query(description="ID первого снэпшота")],
    snapshot_b: Annotated[int, Query(description="ID второго снэпшота")],
    route_key: str | None = None,
    pool: DuckLakePool = Depends(get_pool),
) -> list[SnapshotCompareResult]:
    """
    Сравнить агрегированные метрики между двумя снэпшотами DuckLake.
    Показывает, как изменились данные между двумя точками во времени.
    """

    def _query_snapshot(conn, snap_id: int, route_filter: str | None) -> dict:
        where = f"where route_key = '{route_filter}'" if route_filter else ""
        sql = f"""
            select
                sum(total_bookings)     as total_bookings,
                sum(total_revenue)      as total_revenue,
                sum(total_flights)      as total_flights,
                round(avg(load_factor_pct), 2) as avg_load_factor,
                sum(cancelled_flights)  as cancelled_flights
            from flights.mart_route_daily AT (VERSION => {snap_id})
            {where}
        """
        try:
            row = conn.execute(sql).fetchone()
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"Snapshot {snap_id} not found or inaccessible: {e}",
            )
        return {
            "total_bookings": float(row[0] or 0),
            "total_revenue": float(row[1] or 0),
            "total_flights": float(row[2] or 0),
            "avg_load_factor": float(row[3] or 0),
            "cancelled_flights": float(row[4] or 0),
        }

    with pool.acquire() as conn:
        metrics_a = _query_snapshot(conn, snapshot_a, route_key)
        metrics_b = _query_snapshot(conn, snapshot_b, route_key)

    results = []
    for metric in metrics_a:
        val_a = metrics_a[metric]
        val_b = metrics_b[metric]
        diff = val_b - val_a
        diff_pct = round(diff / val_a * 100, 2) if val_a != 0 else None
        results.append(
            SnapshotCompareResult(
                metric=metric,
                snapshot_a=val_a,
                snapshot_b=val_b,
                diff=round(diff, 2),
                diff_pct=diff_pct,
            )
        )

    return results


@router.get("/price-history/{flight_id}", response_model=list[PricePoint])
def get_flight_price_history(
    flight_id: str,
    pool: DuckLakePool = Depends(get_pool),
) -> list[PricePoint]:
    """
    История цен на конкретный рейс — как менялась цена по мере приближения даты вылета.
    Данные берутся из mart_pricing_analysis, который агрегирован из price_history.
    """
    sql = """
        select
            recorded_at,
            fare_class,
            price_rub,
            days_before_departure
        from flights.price_history
        where flight_id = ?
        order by recorded_at
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [flight_id]).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No price history for flight '{flight_id}'",
        )

    return [
        PricePoint(
            recorded_at=r[0],
            fare_class=r[1],
            price_rub=float(r[2]),
            days_before_departure=int(r[3]),
        )
        for r in rows
    ]


@router.get("/pricing-curves", response_model=list[dict])
def get_pricing_curves(
    route_key: str,
    fare_class: Annotated[str, Query(pattern="^(economy|business|first)$")] = "economy",
    pool: DuckLakePool = Depends(get_pool),
) -> list[dict]:
    """
    Кривые цен по маршруту: средняя цена по бакетам дней до вылета.
    Показывает dynamic pricing в действии.
    """
    sql = """
        select
            days_bucket,
            fare_class,
            avg_price_rub,
            min_price_rub,
            max_price_rub,
            median_price_rub,
            price_observations
        from flights.mart_pricing_analysis
        where route_key = ?
          and fare_class = ?
        order by days_bucket
    """
    with pool.acquire() as conn:
        rows = conn.execute(sql, [route_key, fare_class]).fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No pricing data for route '{route_key}' / fare_class '{fare_class}'",
        )

    cols = [
        "days_bucket", "fare_class", "avg_price_rub", "min_price_rub",
        "max_price_rub", "median_price_rub", "price_observations",
    ]
    return [dict(zip(cols, r)) for r in rows]
