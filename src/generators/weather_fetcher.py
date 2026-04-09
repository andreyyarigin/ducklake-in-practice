"""
weather_fetcher.py — загрузка погодных данных из Open-Meteo Archive API.

Open-Meteo: https://open-meteo.com/
- Полностью бесплатно, без API ключа
- Исторические данные с 1940 года по координатам (lat, lon)
- Агрегированные дневные значения: температура, осадки, ветер, снег, видимость

Для каждого аэропорта из DuckLake загружает наблюдения за указанную дату.

Использование:
    python -m src.generators.weather_fetcher --date 2025-06-15
"""
from __future__ import annotations

import argparse
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime, timezone
from typing import Any

try:
    import json
except ImportError:
    import simplejson as json  # type: ignore

# WMO Weather interpretation codes → описание
# https://open-meteo.com/en/docs#weathervariables
WMO_DESCRIPTIONS: dict[int, str] = {
    0: "Ясно",
    1: "Преимущественно ясно", 2: "Переменная облачность", 3: "Пасмурно",
    45: "Туман", 48: "Туман с инеем",
    51: "Морось слабая", 53: "Морось умеренная", 55: "Морось сильная",
    61: "Дождь слабый", 63: "Дождь умеренный", 65: "Дождь сильный",
    71: "Снег слабый", 73: "Снег умеренный", 75: "Снег сильный",
    77: "Снежная крупа",
    80: "Ливень слабый", 81: "Ливень умеренный", 82: "Ливень сильный",
    85: "Снежный ливень слабый", 86: "Снежный ливень сильный",
    95: "Гроза", 96: "Гроза с градом", 99: "Гроза с сильным градом",
}

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Переменные для запроса (дневные агрегаты)
DAILY_VARIABLES = [
    "temperature_2m_min",
    "temperature_2m_max",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "snowfall_sum",
    "weathercode",
]

# Дополнительно: минимальная видимость из часовых данных
HOURLY_VARIABLES = ["visibility"]


def fetch_weather_for_airport(
    iata_code: str,
    latitude: float,
    longitude: float,
    obs_date: date,
    retries: int = 3,
) -> dict[str, Any] | None:
    """
    Запросить дневную погоду из Open-Meteo Archive API для одного аэропорта.

    Returns:
        Словарь с погодными наблюдениями или None при ошибке.
    """
    date_str = obs_date.isoformat()

    # Запрос дневных агрегатов
    params = {
        "latitude": str(latitude),
        "longitude": str(longitude),
        "start_date": date_str,
        "end_date": date_str,
        "daily": ",".join(DAILY_VARIABLES),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }
    url = f"{OPEN_METEO_URL}?{urllib.parse.urlencode(params)}"

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt)
                continue
            print(f"  HTTP {e.code} for {iata_code}: {e}")
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            print(f"  Error fetching {iata_code}: {e}")
            return None

    daily = data.get("daily", {})
    hourly = data.get("hourly", {})

    # Дневные значения — первый (и единственный) элемент списка
    def _get(key: str) -> Any:
        vals = daily.get(key, [None])
        return vals[0] if vals else None

    # Минимальная видимость из часовых данных (переводим м → км)
    vis_vals = [v for v in hourly.get("visibility", []) if v is not None]
    visibility_min_km = round(min(vis_vals) / 1000.0, 2) if vis_vals else None

    weather_code = _get("weathercode")
    description = WMO_DESCRIPTIONS.get(int(weather_code), "Неизвестно") if weather_code is not None else None

    return {
        "observation_id": str(uuid.uuid4()),
        "airport_iata": iata_code,
        "observation_date": obs_date,
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
        "fetched_at": datetime.now(timezone.utc),
    }


def fetch_weather_for_airports(
    airports: list[dict[str, Any]],
    obs_date: date,
    delay_seconds: float = 0.2,
) -> list[dict[str, Any]]:
    """
    Загрузить погоду для списка аэропортов за указанную дату.

    Open-Meteo бесплатного уровня: ~10000 запросов/день.
    delay_seconds — пауза между запросами чтобы не превысить rate limit.
    """
    observations = []
    for airport in airports:
        iata = airport["iata_code"]
        lat = airport["latitude"]
        lon = airport["longitude"]

        if lat is None or lon is None:
            print(f"  SKIP {iata}: no coordinates")
            continue

        obs = fetch_weather_for_airport(iata, lat, lon, obs_date)
        if obs:
            observations.append(obs)
            print(f"  OK {iata}: {obs['temperature_mean_c']}°C, "
                  f"wind {obs['windspeed_max_kmh']} km/h, "
                  f"{obs['weather_description']}")
        else:
            print(f"  SKIP {iata}: fetch failed")

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return observations


def insert_weather_observations(conn, observations: list[dict[str, Any]]) -> None:
    """
    Вставить наблюдения в DuckLake через temp table + INSERT SELECT.
    Паттерн: temp table → INSERT SELECT (DuckLake не поддерживает executemany напрямую).
    """
    if not observations:
        return

    conn.execute("""
        CREATE TEMP TABLE _tmp_weather (
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
    conn.executemany(
        "INSERT INTO _tmp_weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                o["observation_id"], o["airport_iata"], o["observation_date"],
                o["temperature_min_c"], o["temperature_max_c"], o["temperature_mean_c"],
                o["precipitation_mm"], o["windspeed_max_kmh"], o["windgusts_max_kmh"],
                o["visibility_min_km"], o["snowfall_cm"],
                o["weather_code"], o["weather_description"], o["fetched_at"],
            )
            for o in observations
        ],
    )
    conn.execute(
        "INSERT INTO flights.weather_observations SELECT * FROM _tmp_weather"
    )
    conn.execute("DROP TABLE _tmp_weather")


# ─── CLI точка входа ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch weather data from Open-Meteo")
    parser.add_argument("--date", required=True, help="Observation date YYYY-MM-DD")
    args = parser.parse_args()

    obs_date = date.fromisoformat(args.date)

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    from src.generators.connection import get_ducklake_connection

    conn = get_ducklake_connection()

    airports_raw = conn.execute(
        "SELECT iata_code, latitude, longitude FROM flights.airports"
    ).fetchall()
    airports = [
        {"iata_code": r[0], "latitude": r[1], "longitude": r[2]}
        for r in airports_raw if r[0]
    ]

    print(f"Fetching weather for {len(airports)} airports on {obs_date}...")
    observations = fetch_weather_for_airports(airports, obs_date)

    insert_weather_observations(conn, observations)
    conn.close()

    print(f"\nWeather observations loaded: {len(observations)}/{len(airports)} airports")


if __name__ == "__main__":
    main()
