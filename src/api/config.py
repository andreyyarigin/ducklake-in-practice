"""
config.py — конфигурация FastAPI-приложения из переменных окружения.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.environ.get("DUCKLAKE_PG_HOST", "postgres")
    pg_db: str = os.environ.get("DUCKLAKE_PG_DB", "ducklake_catalog")
    pg_user: str = os.environ.get("DUCKLAKE_PG_USER", "ducklake")
    pg_password: str = os.environ.get("DUCKLAKE_PG_PASSWORD", "ducklake_secret_change_me")

    s3_key: str = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    s3_secret: str = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    s3_endpoint: str = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    s3_bucket: str = os.environ.get("MINIO_BUCKET", "ducklake-flights")

    api_host: str = os.environ.get("API_HOST", "0.0.0.0")
    api_port: int = int(os.environ.get("API_PORT", "8000"))

    # Размер пула read-only соединений
    pool_size: int = int(os.environ.get("API_POOL_SIZE", "4"))

    @property
    def s3_endpoint_host(self) -> str:
        return (
            self.s3_endpoint
            .removeprefix("http://")
            .removeprefix("https://")
        )

    @property
    def ducklake_attach_str(self) -> str:
        return (
            f"ducklake:postgres:host={self.pg_host} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
