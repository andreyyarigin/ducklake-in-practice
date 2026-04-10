"""
dbt-duckdb plugin для автоматического ATTACH DuckLake при инициализации соединения.
Позволяет передать DATA_PATH, который не поддерживается стандартным Attachment.
"""
import os

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    def configure_connection(self, conn):
        pg_host = os.environ.get("DUCKLAKE_PG_HOST", "postgres")
        pg_db = os.environ.get("DUCKLAKE_PG_DB", "ducklake_catalog")
        pg_user = os.environ.get("DUCKLAKE_PG_USER", "ducklake")
        pg_password = os.environ.get("DUCKLAKE_PG_PASSWORD", "ducklake_secret_change_me")
        bucket = os.environ.get("MINIO_BUCKET", "ducklake-flights")

        # Разрешаем spill на диск при нехватке памяти
        conn.execute("SET memory_limit = '6GB'")
        conn.execute("SET temp_directory = '/tmp/duckdb_tmp'")

        attach_sql = (
            f"ATTACH IF NOT EXISTS "
            f"'ducklake:postgres:host={pg_host} port=5432 dbname={pg_db} "
            f"user={pg_user} password={pg_password}' "
            f"AS flights (DATA_PATH 's3://{bucket}/data/')"
        )
        conn.execute(attach_sql)
