#!/usr/bin/env python3
"""
healthcheck.py — проверка подключения DuckDB → DuckLake → MinIO.

Выполнить внутри airflow-worker:
    docker compose exec airflow-worker python /opt/ducklake-in-practice/scripts/healthcheck.py
"""
import os
import sys

import duckdb


def get_env(key: str, default: str | None = None) -> str:
    value = os.environ.get(key, default)
    if value is None:
        print(f"ERROR: environment variable {key} is not set")
        sys.exit(1)
    return value


def main() -> None:
    pg_host = get_env("DUCKLAKE_PG_HOST", "postgres")
    pg_db = get_env("DUCKLAKE_PG_DB", "ducklake_catalog")
    pg_user = get_env("DUCKLAKE_PG_USER", "ducklake")
    pg_password = get_env("DUCKLAKE_PG_PASSWORD", "ducklake_secret_change_me")
    s3_key = get_env("MINIO_ACCESS_KEY", "minioadmin")
    s3_secret = get_env("MINIO_SECRET_KEY", "minioadmin")
    s3_endpoint = get_env("MINIO_ENDPOINT", "http://minio:9000")
    s3_bucket = get_env("MINIO_BUCKET", "ducklake-flights")

    print("Connecting to DuckDB...")
    conn = duckdb.connect()

    print("Installing and loading extensions...")
    for ext in ("ducklake", "postgres", "httpfs"):
        conn.execute(f"INSTALL {ext}; LOAD {ext};")

    print("Configuring S3 secret...")
    conn.execute(f"""
        CREATE SECRET IF NOT EXISTS minio_secret (
            TYPE S3,
            KEY_ID '{s3_key}',
            SECRET '{s3_secret}',
            ENDPOINT '{s3_endpoint.removeprefix("http://").removeprefix("https://")}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)

    attach_str = (
        f"ducklake:postgres:host={pg_host} dbname={pg_db} "
        f"user={pg_user} password={pg_password}"
    )
    print(f"Attaching DuckLake: {attach_str.split('password')[0]}password=***")
    conn.execute(f"""
        ATTACH '{attach_str}'
        AS flights (DATA_PATH 's3://{s3_bucket}/data/')
    """)

    print("Running smoke test (CREATE / INSERT / SELECT / DROP)...")
    conn.execute("CREATE TABLE IF NOT EXISTS flights.healthcheck_test (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO flights.healthcheck_test VALUES (1, 'ok')")
    rows = conn.execute("SELECT * FROM flights.healthcheck_test").fetchall()
    assert rows == [(1, "ok")], f"Unexpected rows: {rows}"
    conn.execute("DROP TABLE flights.healthcheck_test")

    print()
    print("DuckLake connection OK")
    print(f"  PostgreSQL:  {pg_host}:{pg_db}")
    print(f"  MinIO:      {s3_endpoint}/{s3_bucket}")
    print("All checks passed.")


if __name__ == "__main__":
    main()
