# Масштабирование ducklake-in-practice

## Текущие ограничения

### DuckDB: single-node, single-process

DuckDB работает на одной машине. Нет кластера, нет шардирования. Каждый клиент (FastAPI, Superset, dbt) запускает свой DuckDB-процесс. Они не делят вычислительные ресурсы.

**Concurrency:** DuckDB оптимизирован для крупных, редких запросов. Быстрое выполнение множества мелких конкурентных запросов — не его сила. Модель: один writer, множество readers в рамках одного процесса.

**Workaround в проекте:** dbt считает агрегаты заранее → serving-слой читает маленькие mart-таблицы → DuckDB справляется с лёгкими serving-запросами.

### DuckLake: experimental (v0.4)

DuckLake не production-ready (ожидаемая стабилизация: 2026). Основные проблемы:
- Join performance на Parquet хуже, чем native DuckDB (ошибки оценки кардинальности)
- Нет constraints, keys, indexes
- `threads: 1` обязателен — нет concurrent write support
- Не поддерживает CASCADE DROP
- Не поддерживает `executemany()` (нужна temp table + INSERT SELECT)

### Объём данных

Текущий: ~3.5 ГБ/неделю (500 МБ/день × 7 дней TTL raw). DuckDB комфортно работает с данными до ~100-200 ГБ на одной машине (зависит от RAM). При росте за эти пределы нужна другая стратегия.

### Пропускная способность записи

Два Airflow-воркера, `threads: 1` в dbt. Ingestion + transformation — последовательно. При текущем объёме (500 МБ/день) это не bottleneck. При росте в 10x возникнут задержки.

## Пути масштабирования

### Путь 1: Расширение данных (МСК → РФ → СНГ)

**Что меняется:** больше аэропортов, маршрутов, рейсов. Объём данных растёт линейно.

**Что делать:**
- Добавить партиционирование по `src_airport_iata` (второй уровень после `flight_date`)
- Увеличить Airflow-воркеров до 3-4
- Увеличить RAM для DuckDB-контейнеров

**DuckLake справляется:** линейный рост не меняет архитектуру.

### Путь 2: Больше аналитиков (>10 пользователей)

**Проблема:** DuckDB in-process не масштабируется на десятки конкурентных пользователей Superset/FastAPI.

**Вариант A: MotherDuck (managed DuckDB)**

```yaml
# profiles.yml
dev:
  type: duckdb
  path: "md:ducklake_flights?motherduck_token=${MOTHERDUCK_TOKEN}"
```

MotherDuck — серверный DuckDB с конкурентным доступом. Данные остаются в DuckLake. Минимальные изменения в коде. Рекомендуется как первый шаг.

**Вариант B: ClickHouse serving**

```sql
-- ClickHouse читает Parquet из MinIO напрямую
CREATE TABLE mart_route_daily
ENGINE = S3('http://minio:9000/ducklake-flights/data/main/mart_route_daily/*.parquet', 'Parquet')
```

ClickHouse как serving-слой. DuckLake остаётся source of truth. Superset подключается к ClickHouse. Хорошо масштабируется горизонтально.

**Вариант C: PostgreSQL materialized views**

```sql
-- Для простых дашбордов
CREATE MATERIALIZED VIEW mart_route_daily AS
SELECT * FROM ducklake_export('...');

REFRESH MATERIALIZED VIEW CONCURRENTLY mart_route_daily;
```

Для случая, когда нужна привычная PostgreSQL. Нативная поддержка в Superset, не требует новых систем.

### Путь 3: Sub-second latency для API

**Проблема:** DuckDB не OLTP. Для API с жёсткими SLA (~10-50ms) нужен кэш.

**Решение:** Redis-кэш поверх mart-таблиц.

```python
@app.get("/routes/{key}/daily")
async def route_daily(key: str):
    cached = await redis.get(f"route_daily:{key}")
    if cached:
        return json.loads(cached)
    result = duckdb_query(...)
    await redis.setex(f"route_daily:{key}", 300, json.dumps(result))
    return result
```

TTL кэша — 5 минут (mart обновляется ежедневно, кэш не устаревает между обновлениями).

### Путь 4: Real-time (замена batch на streaming)

**Проблема:** данные доступны с задержкой в 1 час.

**Решение:** streaming выходит за рамки DuckLake. Архитектура lambda/kappa:

```
Kafka
  ├── Flink/Spark Streaming → ClickHouse (real-time, <1 мин задержка)
  └── Batch → DuckLake (исторический анализ, time travel)
```

DuckLake остаётся для долгосрочного хранения и аналитики. Real-time serving — отдельный стек.

### Путь 5: >1 ТБ данных

**Проблема:** DuckDB на одной машине ограничен RAM.

**Решение:** переход на Spark + Iceberg или MotherDuck с разделённым compute/storage.

```
DuckLake (Parquet + PG-каталог)
    ↓ (миграция Parquet-файлов)
Iceberg (те же Parquet, другой каталог)
    ↓
Spark (distributed compute)
```

Миграция не требует переформатирования файлов — Parquet остаётся Parquet. Нужно перейти на Iceberg-каталог и настроить Spark-кластер.

## Матрица решений

| Сценарий | Рекомендация | Сложность |
|----------|-------------|-----------|
| 1-5 аналитиков, <100 ГБ | Текущая архитектура | Готово |
| 5-20 аналитиков, <500 ГБ | MotherDuck | Низкая |
| 20+ аналитиков, дашборды | ClickHouse serving | Средняя |
| Sub-second API | Redis cache + marts | Средняя |
| Real-time + analytics | Kafka + ClickHouse + DuckLake | Высокая |
| >1 ТБ данных | MotherDuck или Spark + Iceberg | Высокая |

## Честная оценка

### Где DuckDB/DuckLake отлично работает

- Ad-hoc аналитика для одного аналитика — быстрее, чем что-либо другое
- dbt-трансформации — проще и дешевле, чем Spark
- Локальная разработка — `pip install duckdb` и всё работает
- Операционная простота — нет кластера, нет ZooKeeper, нет coordinator
- Time travel и schema evolution — элегантнее, чем в Iceberg

### Где DuckDB/DuckLake упирается

- High-concurrency serving (>10 конкурентных запросов)
- Данные >200 ГБ на одной машине
- Sub-second latency для API без кэша
- Real-time ingestion
- Multi-writer из разных процессов (DuckLake улучшает через PG, но не решает полностью)
- Production-grade надёжность (v0.4 — experimental)

### Вывод

DuckDB/DuckLake — отличный выбор для sandbox, proof-of-concept и небольших аналитических платформ (до 5 аналитиков, до 100 ГБ). При росте требований стек эволюционирует поэтапно: MotherDuck → ClickHouse serving → Spark + Iceberg. Переход поэтапный: Parquet-файлы не нужно перезаписывать.
