"""
database.py — пул read-only DuckDB-соединений с DuckLake.

DuckDB — embedded, не сервер. Каждое соединение — отдельный in-process движок.
Пул позволяет обрабатывать несколько запросов параллельно без блокировок:
каждый запрос берёт свободное соединение из пула.

Архитектурное ограничение: DuckDB не поддерживает множество concurrent writers
в одном процессе. Здесь все соединения read-only — это безопасно.
"""
from __future__ import annotations

import queue
import threading
from contextlib import contextmanager
from typing import Generator

import duckdb

from src.api.config import settings


class DuckLakePool:
    """Простой пул read-only DuckDB-соединений с DuckLake."""

    def __init__(self, size: int = 4) -> None:
        self._pool: queue.Queue[duckdb.DuckDBPyConnection] = queue.Queue(maxsize=size)
        self._lock = threading.Lock()
        for _ in range(size):
            self._pool.put(self._create_connection())

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()

        for ext in ("ducklake", "postgres", "httpfs"):
            conn.execute(f"INSTALL {ext}; LOAD {ext};")

        conn.execute(f"""
            CREATE SECRET IF NOT EXISTS rustfs_secret (
                TYPE S3,
                KEY_ID '{settings.s3_key}',
                SECRET '{settings.s3_secret}',
                ENDPOINT '{settings.s3_endpoint_host}',
                URL_STYLE 'path',
                USE_SSL false
            )
        """)

        conn.execute(f"""
            ATTACH '{settings.ducklake_attach_str}'
            AS flights (
                DATA_PATH 's3://{settings.s3_bucket}/data/',
                READ_ONLY
            )
        """)

        return conn

    @contextmanager
    def acquire(self, timeout: float = 30.0) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Взять соединение из пула. Вернуть после использования."""
        try:
            conn = self._pool.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError("DuckLake connection pool exhausted. Try again later.")
        try:
            yield conn
        finally:
            self._pool.put(conn)


# Глобальный пул — инициализируется при старте приложения
_pool: DuckLakePool | None = None


def init_pool() -> None:
    global _pool
    _pool = DuckLakePool(size=settings.pool_size)


def get_pool() -> DuckLakePool:
    if _pool is None:
        raise RuntimeError("Pool not initialized. Call init_pool() first.")
    return _pool
