"""
backfill_weather.py — загрузка исторической погоды за весь период рейсов.

Запрашивает Open-Meteo Archive API: один запрос на аэропорт охватывает
весь диапазон дат (эффективнее поддённых запросов из weather_fetcher.py).

Использование:
    python /opt/ducklake-in-practice/scripts/backfill_weather.py
    python /opt/ducklake-in-practice/scripts/backfill_weather.py --start 2025-01-01 --end 2025-03-31
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.connection import get_ducklake_connection
from src.generators.weather_fetcher import WMO_DESCRIPTIONS

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARIABLES = [
    "temperature_2m_min", "temperature_2m_max", "temperature_2m_mean",
    "precipitation_sum", "windspeed_10m_max", "windgusts_10m_max",
    "snowfall_sum", "weathercode",
]
HOURLY_VARIABLES = ["visibility"]


def fetch_weather_range(
    iata_code: str,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    retries: int = 3,
) -> list[dict[str, Any]]:
    """Запросить погоду для одного аэропорта за диапазон дат (один HTTP запрос)."""
    params = {
        "latitude": str(latitude),
        "longitude": str(longitude),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }
    url = f"{OPEN_METEO_URL}?{urllib.parse.urlencode(params)}"

    data = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f"  HTTP {e.code} for {iata_code}: {e}")
            return []
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            print(f"  Error {iata_code}: {e}")
            return []

    if data is None:
        return []

    daily = data.get("daily", {})
    hourly = data.get("hourly", {})
    dates = daily.get("time", [])

    # Группируем часовые данные видимости по дате
    hourly_times = hourly.get("time", [])
    hourly_vis = hourly.get("visibility", [])
    vis_by_date: dict[str, list[float]] = {}
    for t, v in zip(hourly_times, hourly_vis):
        d = t[:10]  # "YYYY-MM-DD"
        if v is not None:
            vis_by_date.setdefault(d, []).append(v)

    observations = []
    fetched_at = datetime.now(timezone.utc)

    for i, d_str in enumerate(dates):
        def _get(key: str) -> Any:
            vals = daily.get(key, [])
            return vals[i] if i < len(vals) else None

        weather_code = _get("weathercode")
        description = (
            WMO_DESCRIPTIONS.get(int(weather_code), "Неизвестно")
            if weather_code is not None else None
        )

        vis_vals = vis_by_date.get(d_str, [])
        visibility_min_km = round(min(vis_vals) / 1000.0, 2) if vis_vals else None

        observations.append({
            "observation_id": str(uuid.uuid4()),
            "airport_iata": iata_code,
            "observation_date": date.fromisoformat(d_str),
            "temperature_min_c": _get("temperature_2m_min"),
            "temperature_max_c": _get("temperature_2m_max"),
            "temperature_mean_c": _get("temperature_2m_mean"),
            "precipitation_mm": _get("precipitation_sum"),
            "windspeed_max_kmh": _get("windspeed_10m_max"),
            "windgusts_max_kmh": _get("windgusts_10m_max"),
            "visibility_min_km": visibility_min_km,
            "snowfall_cm": _get("snowfall_sum"),
            "weather_code": weather_code,
            "weather_description": description,
            "fetched_at": fetched_at,
        })

    return observations


def insert_batch(conn, observations: list[dict[str, Any]]) -> None:
    """Вставка через temp table + INSERT SELECT (паттерн DuckLake)."""
    if not observations:
        return

    conn.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _tmp_weather_bf (
            observation_id      VARCHAR,
            airport_iata        VARCHAR,
            observation_date    DATE,
            temperature_min_c   DOUBLE,
            temperature_max_c   DOUBLE,
            temperature_mean_c  DOUBLE,
            precipitation_mm    DOUBLE,
            windspeed_max_kmh   DOUBLE,
            windgusts_max_kmh   DOUBLE,
            visibility_min_km   DOUBLE,
            snowfall_cm         DOUBLE,
            weather_code        INTEGER,
            weather_description VARCHAR,
            fetched_at          TIMESTAMP
        )
    """)
    conn.execute("DELETE FROM _tmp_weather_bf")

    values_sql = ", ".join(
        f"('{o['observation_id']}', '{o['airport_iata']}', '{o['observation_date']}', "
        f"{o['temperature_min_c'] if o['temperature_min_c'] is not None else 'NULL'}, "
        f"{o['temperature_max_c'] if o['temperature_max_c'] is not None else 'NULL'}, "
        f"{o['temperature_mean_c'] if o['temperature_mean_c'] is not None else 'NULL'}, "
        f"{o['precipitation_mm'] if o['precipitation_mm'] is not None else 'NULL'}, "
        f"{o['windspeed_max_kmh'] if o['windspeed_max_kmh'] is not None else 'NULL'}, "
        f"{o['windgusts_max_kmh'] if o['windgusts_max_kmh'] is not None else 'NULL'}, "
        f"{o['visibility_min_km'] if o['visibility_min_km'] is not None else 'NULL'}, "
        f"{o['snowfall_cm'] if o['snowfall_cm'] is not None else 'NULL'}, "
        f"{o['weather_code'] if o['weather_code'] is not None else 'NULL'}, "
        f"{'NULL' if o['weather_description'] is None else repr(o['weather_description'])}, "
        f"'{o['fetched_at'].strftime('%Y-%m-%d %H:%M:%S')}')"
        for o in observations
    )
    conn.execute(f"INSERT INTO _tmp_weather_bf VALUES {values_sql}")
    conn.execute(
        "INSERT INTO flights.weather_observations SELECT * FROM _tmp_weather_bf"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill weather for all flight dates")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: auto from flights)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: auto from flights)")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between airport requests (s)")
    args = parser.parse_args()

    conn = get_ducklake_connection()

    # Определяем диапазон дат рейсов (если не указан явно)
    if args.start and args.end:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    else:
        row = conn.execute(
            "SELECT MIN(flight_date), MAX(flight_date) FROM flights.flights"
        ).fetchone()
        if not row or row[0] is None:
            print("No flights found. Exiting.")
            conn.close()
            return
        start_date = row[0]
        end_date = row[1]

    print(f"Weather backfill: {start_date} → {end_date}")

    # Аэропорты, задействованные в рейсах (экономим запросы)
    airports_rows = conn.execute("""
        SELECT DISTINCT a.iata_code, a.latitude, a.longitude
        FROM flights.airports a
        WHERE a.iata_code IN (
            SELECT src_airport_iata FROM flights.flights
            UNION
            SELECT dst_airport_iata FROM flights.flights
        )
        AND a.latitude IS NOT NULL
        AND a.longitude IS NOT NULL
        ORDER BY a.iata_code
    """).fetchall()

    airports = [{"iata_code": r[0], "latitude": r[1], "longitude": r[2]} for r in airports_rows]
    print(f"Airports in use: {len(airports)}")

    # Удаляем существующие данные за этот период чтобы избежать дублей
    conn.execute(
        "DELETE FROM flights.weather_observations WHERE observation_date BETWEEN ? AND ?",
        [start_date, end_date],
    )
    print(f"Cleared existing weather for {start_date} → {end_date}")

    total_inserted = 0
    for i, airport in enumerate(airports, 1):
        iata = airport["iata_code"]
        print(f"  [{i}/{len(airports)}] {iata}...", end=" ", flush=True)
        observations = fetch_weather_range(
            iata, airport["latitude"], airport["longitude"],
            start_date, end_date,
        )
        if observations:
            insert_batch(conn, observations)
            print(f"{len(observations)} days OK")
            total_inserted += len(observations)
        else:
            print("SKIP")

        if args.delay > 0:
            time.sleep(args.delay)

    conn.close()
    print(f"\nDone. Total observations inserted: {total_inserted}")
    days = (end_date - start_date).days + 1
    print(f"Coverage: {len(airports)} airports × {days} days = {len(airports) * days} expected")


if __name__ == "__main__":
    main()
