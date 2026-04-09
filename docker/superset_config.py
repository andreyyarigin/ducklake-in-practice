"""
superset_config.py — конфигурация Apache Superset для ducklake-in-practice.

Superset подключается к serving store — обычному DuckDB файлу /serving/flights.duckdb.
Файл содержит экспортированные mart-таблицы из DuckLake и обновляется по расписанию
через Airflow DAG (dag_export_serving_store).

URI в Superset UI: duckdb:////serving/flights.duckdb
"""
import os

# ─── Superset metadata DB ─────────────────────────────────────────────────────
_pg_pass = os.environ.get("SUPERSET_DB_PASSWORD", "superset_secret_change_me")
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{_pg_pass}@postgres:5432/superset_appdb"
)

# ─── Security ─────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "superset_secret_change_me_32chars!")
WTF_CSRF_ENABLED = True

# ─── Allow DuckDB connections (not in default allowlist in Superset 4.x) ──────
PREVENT_UNSAFE_DB_CONNECTIONS = False

# ─── Redis cache ──────────────────────────────────────────────────────────────
# Airflow uses Redis DB 0 — Superset uses DB 1 to avoid collisions
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://redis:6379/1",
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# ─── Celery (async queries) ───────────────────────────────────────────────────
class CeleryConfig:
    broker_url = "redis://redis:6379/1"
    result_backend = "redis://redis:6379/1"
    worker_prefetch_multiplier = 1
    task_acks_late = True

CELERY_CONFIG = CeleryConfig
