"""
load_aircraft_fleet.py — загрузка справочника типов воздушных судов в DuckLake.

Читает data/seeds/aircraft_types.csv и загружает в таблицу flights.aircraft_types.
Содержит публичные технические характеристики ВС: вместимость, дальность,
расход топлива, тип двигателя — данные из открытых источников (Airbus, Boeing,
Sukhoi technical documentation).

Использование:
    python /opt/ducklake-in-practice/scripts/load_aircraft_fleet.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.connection import get_ducklake_connection

AIRCRAFT_CSV = Path(__file__).parent.parent / "data" / "seeds" / "aircraft_types.csv"


def _int(val: str) -> int | None:
    v = val.strip()
    return int(v) if v else None


def _float(val: str) -> float | None:
    v = val.strip()
    return float(v) if v else None


def _str(val: str) -> str | None:
    v = val.strip()
    return v if v else None


def load_aircraft_fleet(conn) -> int:
    with open(AIRCRAFT_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)

    conn.execute("DELETE FROM flights.aircraft_types")
    conn.execute("""
        CREATE TEMP TABLE _tmp_aircraft (
            icao_code           VARCHAR,
            iata_code           VARCHAR,
            manufacturer        VARCHAR,
            model               VARCHAR,
            family              VARCHAR,
            seats_economy       INTEGER,
            seats_business      INTEGER,
            seats_first         INTEGER,
            seats_total         INTEGER,
            range_km            INTEGER,
            fuel_burn_kg_per_km DOUBLE,
            first_flight_year   INTEGER,
            engine_type         VARCHAR,
            body_type           VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_aircraft VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                _str(r["icao_code"]),
                _str(r["iata_code"]),
                _str(r["manufacturer"]),
                _str(r["model"]),
                _str(r["family"]),
                _int(r["seats_economy"]),
                _int(r["seats_business"]),
                _int(r["seats_first"]),
                _int(r["seats_total"]),
                _int(r["range_km"]),
                _float(r["fuel_burn_kg_per_km"]),
                _int(r["first_flight_year"]),
                _str(r["engine_type"]),
                _str(r["body_type"]),
            )
            for r in records
        ],
    )
    conn.execute("INSERT INTO flights.aircraft_types SELECT * FROM _tmp_aircraft")
    conn.execute("DROP TABLE _tmp_aircraft")
    return len(records)


def main() -> None:
    print("Connecting to DuckLake...")
    conn = get_ducklake_connection()

    print(f"Loading aircraft fleet from {AIRCRAFT_CSV}...")
    count = load_aircraft_fleet(conn)
    conn.close()

    print(f"Aircraft fleet loaded: {count} aircraft types")


if __name__ == "__main__":
    main()
