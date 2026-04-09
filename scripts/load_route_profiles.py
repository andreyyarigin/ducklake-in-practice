"""
load_route_profiles.py — загрузка профилей маршрутов в DuckLake.

Профили маршрутов определяют реалистичные характеристики каждого маршрута:
  - base_load_factor: базовый коэффициент загрузки (0.45–0.88)
  - price_tier: ценовой уровень (budget/medium/premium)
  - seasonality_type: тип сезонности (low/high_summer)
  - competition_level: уровень конкуренции (low/medium/high)

Данные основаны на географии РФ и публичной статистике пассажиропотока
(Росавиация, открытые данные аэропортов).

Использование:
    python /opt/ducklake-in-practice/scripts/load_route_profiles.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.generators.connection import get_ducklake_connection

ROUTE_PROFILES_CSV = Path(__file__).parent.parent / "data" / "seeds" / "route_profiles.csv"


def load_route_profiles(conn) -> int:
    with open(ROUTE_PROFILES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = [r for r in reader if r["src_iata"].strip()]

    conn.execute("DELETE FROM flights.route_profiles")
    conn.execute("""
        CREATE TEMP TABLE _tmp_route_profiles (
            src_iata          VARCHAR,
            dst_iata          VARCHAR,
            base_load_factor  DOUBLE,
            price_tier        VARCHAR,
            seasonality_type  VARCHAR,
            competition_level VARCHAR,
            notes             VARCHAR
        )
    """)
    conn.executemany(
        "INSERT INTO _tmp_route_profiles VALUES (?,?,?,?,?,?,?)",
        [
            (
                r["src_iata"].strip(),
                r["dst_iata"].strip(),
                float(r["base_load_factor"]),
                r["price_tier"].strip(),
                r["seasonality_type"].strip(),
                r["competition_level"].strip(),
                r["notes"].strip() or None,
            )
            for r in records
        ],
    )
    conn.execute("INSERT INTO flights.route_profiles SELECT * FROM _tmp_route_profiles")
    conn.execute("DROP TABLE _tmp_route_profiles")
    return len(records)


def main() -> None:
    print("Connecting to DuckLake...")
    conn = get_ducklake_connection()

    print(f"Loading route profiles from {ROUTE_PROFILES_CSV}...")
    count = load_route_profiles(conn)
    conn.close()

    print(f"Route profiles loaded: {count} routes")


if __name__ == "__main__":
    main()
