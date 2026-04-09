"""
main.py — FastAPI-приложение для serving аналитики DuckLake.

Архитектура:
  - DuckDB in-process, read-only пул соединений
  - Все запросы идут в mart-таблицы (pre-aggregated dbt)
  - Time travel через DuckLake snapshot API
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.database import get_pool, init_pool
from src.api.routers import airlines, routes, time_travel


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: инициализировать пул DuckDB-соединений
    print("Initializing DuckLake connection pool...")
    init_pool()
    print("DuckLake pool ready.")
    yield
    # Shutdown: ничего не нужно — DuckDB in-process


app = FastAPI(
    title="DuckLake Flights API",
    description=(
        "Аналитический API поверх DuckLake lakehouse. "
        "Все данные — внутренние рейсы РФ, агрегированы через dbt."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(routes.router)
app.include_router(airlines.router)
app.include_router(time_travel.router)


@app.get("/health", tags=["system"])
def health_check() -> JSONResponse:
    """Проверка работоспособности API и соединения с DuckLake."""
    try:
        pool = get_pool()
        with pool.acquire(timeout=5.0) as conn:
            result = conn.execute(
                "select count(*) from flights.flights limit 1"
            ).fetchone()
        flight_count = int(result[0]) if result else 0
        return JSONResponse({
            "status": "ok",
            "ducklake": "connected",
            "flights_count": flight_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "ducklake": "unavailable",
                "detail": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )


@app.get("/", tags=["system"])
def root() -> dict:
    return {
        "name": "DuckLake Flights API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "routes": "/routes",
            "routes_top": "/routes/top",
            "airlines": "/airlines",
            "airlines_stats": "/airlines/stats",
            "time_travel_snapshots": "/time-travel/snapshots",
            "time_travel_compare": "/time-travel/compare",
            "pricing_curves": "/time-travel/pricing-curves",
        },
    }
