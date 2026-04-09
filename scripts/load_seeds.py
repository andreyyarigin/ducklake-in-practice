"""
load_seeds.py — загрузка справочных данных OpenFlights в DuckLake.

Скачивает airports.dat, airlines.dat, routes.dat с GitHub OpenFlights,
фильтрует по России и загружает в таблицы DuckLake.

Использование:
    python /opt/ducklake-in-practice/scripts/load_seeds.py
    python /opt/ducklake-in-practice/scripts/load_seeds.py --offline  # из локальных CSV
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import urllib.request
from pathlib import Path

# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.connection import get_ducklake_connection
from src.generators.schema import create_schema
from scripts.load_aircraft_fleet import load_aircraft_fleet
from scripts.load_route_profiles import load_route_profiles

# ─── URLs OpenFlights ──────────────────────────────────────────────────────────

OPENFLIGHTS_BASE = "https://raw.githubusercontent.com/jpatokal/openflights/master/data"
AIRPORTS_URL = f"{OPENFLIGHTS_BASE}/airports.dat"
AIRLINES_URL = f"{OPENFLIGHTS_BASE}/airlines.dat"
ROUTES_URL = f"{OPENFLIGHTS_BASE}/routes.dat"

LOCAL_SEEDS_DIR = Path(__file__).parent.parent / "data" / "seeds"


# ─── Загрузка данных ───────────────────────────────────────────────────────────

def _fetch(url: str, local_path: Path, offline: bool) -> list[list[str]]:
    """Получить CSV-данные: из сети или из локального файла."""
    if offline or local_path.exists():
        if local_path.exists():
            print(f"  Using local: {local_path}")
            with open(local_path, encoding="utf-8", errors="replace") as f:
                return list(csv.reader(f))
        else:
            print(f"ERROR: offline mode but {local_path} not found")
            sys.exit(1)

    print(f"  Downloading: {url}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            content = resp.read().decode("utf-8", errors="replace")
        # Кешируем локально
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(content, encoding="utf-8")
        return list(csv.reader(io.StringIO(content)))
    except Exception as e:
        print(f"ERROR downloading {url}: {e}")
        sys.exit(1)


def _null(val: str) -> str | None:
    v = val.strip().strip('"')
    return None if v in ("", "\\N", "N/A", "Unknown") else v


def _int(val: str) -> int | None:
    v = _null(val)
    if v is None:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def _float(val: str) -> float | None:
    v = _null(val)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


# ─── Парсеры ───────────────────────────────────────────────────────────────────

def parse_airports(rows: list[list[str]]) -> list[dict]:
    """
    Формат airports.dat (14 полей, без заголовка):
    id, name, city, country, iata, icao, lat, lon, alt, tz_offset, DST, tz_name, type, source
    """
    airports = []
    for row in rows:
        if len(row) < 11:
            continue
        country = _null(row[3])
        if country != "Russia":
            continue
        iata = _null(row[4])
        if not iata or len(iata) != 3:
            continue
        airports.append({
            "airport_id": _int(row[0]),
            "name": _null(row[1]),
            "city": _null(row[2]),
            "country": country,
            "iata_code": iata,
            "icao_code": _null(row[5]),
            "latitude": _float(row[6]),
            "longitude": _float(row[7]),
            "altitude": _int(row[8]),
            "timezone_offset": _float(row[9]),
            "timezone_name": _null(row[11]) if len(row) > 11 else None,
        })
    return airports


def parse_airlines(rows: list[list[str]]) -> list[dict]:
    """
    Формат airlines.dat (8 полей, без заголовка):
    id, name, alias, iata, icao, callsign, country, active
    """
    airlines = []
    for row in rows:
        if len(row) < 8:
            continue
        country = _null(row[6])
        if country != "Russia":
            continue
        active_val = _null(row[7])
        active = active_val == "Y" if active_val else False
        if not active:
            continue
        iata = _null(row[3])
        if not iata:
            continue
        airlines.append({
            "airline_id": _int(row[0]),
            "name": _null(row[1]),
            "iata_code": iata,
            "icao_code": _null(row[4]),
            "country": country,
            "active": active,
        })
    return airlines


def parse_routes(
    rows: list[list[str]],
    russian_iatas: set[str],
) -> list[dict]:
    """
    Формат routes.dat (9 полей, без заголовка):
    airline, airline_id, src_airport, src_id, dst_airport, dst_id, codeshare, stops, equipment
    """
    routes = []
    for row in rows:
        if len(row) < 9:
            continue
        airline = _null(row[0])
        src = _null(row[2])
        dst = _null(row[4])
        if not airline or not src or not dst:
            continue
        # Только внутренние рейсы РФ
        if src not in russian_iatas or dst not in russian_iatas:
            continue
        stops_val = _int(row[7])
        if stops_val is None:
            stops_val = 0
        routes.append({
            "airline_iata": airline,
            "src_airport_iata": src,
            "dst_airport_iata": dst,
            "codeshare": _null(row[6]) == "Y",
            "stops": stops_val,
            "equipment": _null(row[8]),
        })
    return routes


# ─── Вставка в DuckLake ───────────────────────────────────────────────────────

def load_airports(conn, airports: list[dict]) -> None:
    conn.execute("DELETE FROM flights.airports")
    conn.execute("""
        CREATE TEMP TABLE _tmp_airports (
            airport_id INTEGER, name VARCHAR, city VARCHAR, country VARCHAR,
            iata_code VARCHAR, icao_code VARCHAR, latitude DOUBLE, longitude DOUBLE,
            altitude INTEGER, timezone_offset DOUBLE, timezone_name VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_airports VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                a["airport_id"], a["name"], a["city"], a["country"],
                a["iata_code"], a["icao_code"], a["latitude"], a["longitude"],
                a["altitude"], a["timezone_offset"], a["timezone_name"],
            )
            for a in airports
        ],
    )
    conn.execute("INSERT INTO flights.airports SELECT * FROM _tmp_airports")
    conn.execute("DROP TABLE _tmp_airports")
    print(f"  Loaded {len(airports)} airports")


def load_airlines(conn, airlines: list[dict]) -> None:
    conn.execute("DELETE FROM flights.airlines")
    conn.execute("""
        CREATE TEMP TABLE _tmp_airlines (
            airline_id INTEGER, name VARCHAR, iata_code VARCHAR,
            icao_code VARCHAR, country VARCHAR, active BOOLEAN
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_airlines VALUES (?,?,?,?,?,?)",
        [
            (
                a["airline_id"], a["name"], a["iata_code"],
                a["icao_code"], a["country"], a["active"],
            )
            for a in airlines
        ],
    )
    conn.execute("INSERT INTO flights.airlines SELECT * FROM _tmp_airlines")
    conn.execute("DROP TABLE _tmp_airlines")
    print(f"  Loaded {len(airlines)} airlines")


def load_routes(conn, routes: list[dict]) -> None:
    conn.execute("DELETE FROM flights.routes")
    conn.execute("""
        CREATE TEMP TABLE _tmp_routes (
            airline_iata VARCHAR, src_airport_iata VARCHAR, dst_airport_iata VARCHAR,
            codeshare BOOLEAN, stops INTEGER, equipment VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_routes VALUES (?,?,?,?,?,?)",
        [
            (
                r["airline_iata"], r["src_airport_iata"], r["dst_airport_iata"],
                r["codeshare"], r["stops"], r["equipment"],
            )
            for r in routes
        ],
    )
    conn.execute("INSERT INTO flights.routes SELECT * FROM _tmp_routes")
    conn.execute("DROP TABLE _tmp_routes")
    print(f"  Loaded {len(routes)} routes")


# ─── Точка входа ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load OpenFlights seed data into DuckLake")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use local CSV files from data/seeds/ instead of downloading",
    )
    args = parser.parse_args()

    print("Connecting to DuckLake...")
    conn = get_ducklake_connection()

    print("Creating schema...")
    create_schema(conn)

    print("Loading airports...")
    airports_rows = _fetch(AIRPORTS_URL, LOCAL_SEEDS_DIR / "airports.dat", args.offline)
    airports = parse_airports(airports_rows)
    load_airports(conn, airports)

    russian_iatas = {a["iata_code"] for a in airports}

    print("Loading airlines...")
    airlines_rows = _fetch(AIRLINES_URL, LOCAL_SEEDS_DIR / "airlines.dat", args.offline)
    airlines = parse_airlines(airlines_rows)
    load_airlines(conn, airlines)

    print("Loading routes...")
    routes_rows = _fetch(ROUTES_URL, LOCAL_SEEDS_DIR / "routes.dat", args.offline)
    routes = parse_routes(routes_rows, russian_iatas)
    load_routes(conn, routes)

    print("Loading aircraft fleet...")
    aircraft_count = load_aircraft_fleet(conn)

    print("Loading route profiles...")
    route_profiles_count = load_route_profiles(conn)

    conn.close()

    print()
    print("Seed data loaded successfully.")
    print(f"  Airports         : {len(airports)}")
    print(f"  Airlines         : {len(airlines)}")
    print(f"  Routes           : {len(routes)}")
    print(f"  Aircraft types   : {aircraft_count}")
    print(f"  Route profiles   : {route_profiles_count}")


if __name__ == "__main__":
    main()
