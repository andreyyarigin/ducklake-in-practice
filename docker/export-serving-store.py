"""
export-serving-store.py — экспорт mart-таблиц из DuckLake в serving store.

Serving store = обычный DuckDB файл (/serving/flights.duckdb).
Superset и другие BI-инструменты читают из него напрямую через стандартный DuckDB driver.

Паттерн атомарного обновления:
  1. Экспортируем в flights_new.duckdb
  2. Атомарно переименовываем: flights_new.duckdb → flights.duckdb
  3. Superset подхватывает новый файл при следующем соединении

Так serving store никогда не бывает в частично обновлённом состоянии.
"""
import os
import shutil
import duckdb

SERVING_DIR = "/serving"
DB_PATH = f"{SERVING_DIR}/flights.duckdb"
DB_PATH_NEW = f"{SERVING_DIR}/flights_new.duckdb"

S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
S3_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
S3_SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
S3_BUCKET = os.environ.get("MINIO_BUCKET", "ducklake-flights")
PG_HOST = os.environ.get("DUCKLAKE_PG_HOST", "postgres")
PG_DB = os.environ.get("DUCKLAKE_PG_DB", "ducklake_catalog")
PG_USER = os.environ.get("DUCKLAKE_PG_USER", "ducklake")
PG_PASSWORD = os.environ.get("DUCKLAKE_PG_PASSWORD", "ducklake_secret_change_me")

# mart_passenger_segments исключён: 6.5M строк (по одной на пассажира) — слишком
# большой для serving store. Для дашборда используется mart_passenger_segment_stats
# (агрегат по сегментам, 4 строки).
EXPORT_TABLES = [
    "mart_route_daily",
    "mart_route_weekly",
    "mart_route_monthly",
    "mart_airline_daily",
    "mart_airport_daily",
    "mart_booking_funnel",
    "mart_pricing_analysis",
    "mart_delay_analysis",
    "mart_weather_delay",
    "mart_passenger_segment_stats",
]

os.makedirs(SERVING_DIR, exist_ok=True)

# Удаляем незавершённый файл от предыдущего упавшего запуска, если есть
if os.path.exists(DB_PATH_NEW):
    os.remove(DB_PATH_NEW)

conn = duckdb.connect()
conn.execute("INSTALL ducklake; LOAD ducklake;")
conn.execute("INSTALL postgres; LOAD postgres;")
conn.execute("INSTALL httpfs; LOAD httpfs;")

conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}'")
conn.execute(f"SET s3_access_key_id='{S3_KEY}'")
conn.execute(f"SET s3_secret_access_key='{S3_SECRET}'")
conn.execute("SET s3_use_ssl=false")
conn.execute("SET s3_url_style='path'")

# Source: DuckLake (read-only)
conn.execute(f"""
    ATTACH IF NOT EXISTS
    'ducklake:postgres:host={PG_HOST} port=5432 dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}'
    AS flights (DATA_PATH 's3://{S3_BUCKET}/data/', READ_ONLY)
""")

# Destination: новый serving store файл
conn.execute(f"ATTACH '{DB_PATH_NEW}' AS serving")

available = {
    row[0] for row in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='flights' AND table_schema='main'"
    ).fetchall()
}

exported = 0
for tbl in EXPORT_TABLES:
    if tbl not in available:
        print(f"  SKIP {tbl} (not in DuckLake yet)")
        continue
    try:
        conn.execute(f"DROP TABLE IF EXISTS serving.main.{tbl}")
        conn.execute(f"CREATE TABLE serving.main.{tbl} AS SELECT * FROM flights.main.{tbl}")
        cnt = conn.execute(f"SELECT COUNT(*) FROM serving.main.{tbl}").fetchone()[0]
        print(f"  OK {tbl}: {cnt} rows")
        exported += 1
    except Exception as e:
        print(f"  ERROR {tbl}: {e}")

conn.close()

if exported == 0:
    print("No tables exported — aborting without replacing serving store.")
    os.remove(DB_PATH_NEW)
else:
    # Атомарная замена: новый файл становится serving store
    shutil.move(DB_PATH_NEW, DB_PATH)
    # Права 666 чтобы Superset (uid=1000) мог открыть файл для WAL
    os.chmod(DB_PATH, 0o666)
    print(f"\nServing store updated: {DB_PATH} ({exported} tables)")
