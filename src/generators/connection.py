"""
connection.py — фабрика DuckDB-соединения с DuckLake + RustFS.

Используется всеми генераторами и скриптами загрузки seed-данных.
"""
from __future__ import annotations

import os

import duckdb


def get_ducklake_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Создать DuckDB-соединение с DuckLake через PostgreSQL catalog и RustFS."""
    pg_host = os.environ.get("DUCKLAKE_PG_HOST", "postgres")
    pg_db = os.environ.get("DUCKLAKE_PG_DB", "ducklake_catalog")
    pg_user = os.environ.get("DUCKLAKE_PG_USER", "ducklake")
    pg_password = os.environ.get("DUCKLAKE_PG_PASSWORD", "ducklake_secret_change_me")
    s3_key = os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin")
    s3_secret = os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin123")
    s3_endpoint = os.environ.get("RUSTFS_ENDPOINT", "http://rustfs:9000")
    s3_bucket = os.environ.get("RUSTFS_BUCKET", "ducklake-flights")

    # Убираем схему из endpoint для DuckDB
    endpoint_host = (
        s3_endpoint
        .removeprefix("http://")
        .removeprefix("https://")
    )

    conn = duckdb.connect()

    for ext in ("ducklake", "postgres", "httpfs"):
        conn.execute(f"INSTALL {ext}; LOAD {ext};")

    conn.execute(f"""
        CREATE SECRET IF NOT EXISTS rustfs_secret (
            TYPE S3,
            KEY_ID '{s3_key}',
            SECRET '{s3_secret}',
            ENDPOINT '{endpoint_host}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)
    # Отключаем multipart upload — используем single-part для надёжности
    conn.execute("SET s3_uploader_max_parts_per_file = 1")
    conn.execute("SET s3_uploader_max_filesize = '5GB'")

    attach_str = (
        f"ducklake:postgres:host={pg_host} dbname={pg_db} "
        f"user={pg_user} password={pg_password}"
    )
    options = f"DATA_PATH 's3://{s3_bucket}/data/'"
    if read_only:
        options += ", READ_ONLY"

    conn.execute(f"ATTACH '{attach_str}' AS flights ({options})")

    return conn
