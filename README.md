# ducklake-in-practice

<img src="docs/cover.webp" width="512" alt="DuckLake Air Traffic Control"/>

**Production-grade lakehouse sandbox на DuckDB + DuckLake**

Аналитическая платформа авиабронирований внутренних рейсов РФ, демонстрирующая возможности и ограничения экосистемы DuckDB для построения полноценного lakehouse.

## О проекте

Исследование возможностей и ограничений DuckLake — нового lakehouse-формата от команды DuckDB.

Полный аналитический стек на синтетических данных внутренних авиарейсов РФ: от ingestion и dbt-трансформаций до BI-дашбордов и REST API. ACID-транзакции, time travel, schema evolution — всё на реальных примерах. Все подводные камни задокументированы.

## Что внутри

- Синтетические данные внутренних рейсов РФ: рейсы, бронирования, история цен, погода
- Полный lakehouse-стек: ingestion → staging → enrichment → marts → serving

## Стек

| Компонент | Технология | Роль |
|-----------|-----------|------|
| Object storage | MinIO | S3-совместимое хранилище Parquet-файлов |
| Metadata catalog | PostgreSQL | Каталог DuckLake + метаданные Airflow + Superset appdb |
| Lakehouse format | DuckLake | ACID, time travel, schema evolution, partitioning |
| Compute engine | DuckDB | Запись, трансформации, чтение — in-process |
| Orchestration | Airflow | Оркестрация ingestion, трансформаций и экспорта |
| Transformations | dbt (dbt-duckdb) | staging → intermediate → marts |
| BI | Apache Superset | Дашборды поверх serving store |
| API | FastAPI | REST-аналитика + time travel демо |

## Быстрый старт

```bash
git clone <repo-url> && cd ducklake-in-practice
cp .env.example .env
docker compose up -d
make seeds                                        # справочники: аэропорты, авиакомпании, маршруты
make backfill FROM=2026-01-01 TO=2026-04-08      # исторические данные
make dbt-run                                      # трансформации (staging → marts)
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

> Подробный гайд с описанием каждого шага, troubleshooting и ежедневным циклом: **[docs/ru/QUICKSTART.md](docs/ru/QUICKSTART.md)**

## Интерфейсы

| Сервис | URL | Credentials |
|--------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| FastAPI docs | http://localhost:8000/docs | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5433 | ducklake / (см. .env) |

## Документация

### Гайды

| Документ | Описание |
|----------|----------|
| [docs/ru/QUICKSTART.md](docs/ru/QUICKSTART.md) | Пошаговый запуск: от клонирования до дашбордов (RU) |
| [docs/en/QUICKSTART.md](docs/en/QUICKSTART.md) | Step-by-step setup guide (EN) |

### Архитектура и устройство

| Документ | Описание |
|----------|----------|
| [docs/ru/ARCHITECTURE.md](docs/ru/ARCHITECTURE.md) | Архитектура, сервисы, принятые решения, уроки первого запуска |
| [docs/ru/DATA_MODEL.md](docs/ru/DATA_MODEL.md) | Модель данных, все таблицы, поля, партиционирование |
| [docs/ru/DBT_LAYERS.md](docs/ru/DBT_LAYERS.md) | Слои dbt, все модели, критические фиксы для DuckLake |
| [docs/ru/DUCKLAKE_FEATURES.md](docs/ru/DUCKLAKE_FEATURES.md) | ACID-транзакции, time travel, сравнение с Iceberg |
| [docs/ru/SCALING.md](docs/ru/SCALING.md) | Ограничения и пути масштабирования |

### Диаграммы

| Диаграмма | PNG | Описание |
|-----------|-----|----------|
| [01\_architecture.puml](docs/diagrams/01_architecture.puml) | [PNG](docs/diagrams/architecture.png) | Архитектура по слоям: оркестрация → DuckLake → dbt → serving → clients |
| [02\_data\_model.puml](docs/diagrams/02_data_model.puml) | [PNG](docs/diagrams/data_model.png) | Модель данных: таблицы, поля, FK-связи |
| [03\_dbt\_lineage.puml](docs/diagrams/03_dbt_lineage.puml) | [PNG](docs/diagrams/dbt_lineage.png) | Граф зависимостей dbt-моделей (raw → staging → intermediate → marts) |
| [04\_airflow\_dags.puml](docs/diagrams/04_airflow_dags.puml) | [PNG](docs/diagrams/airflow_dags.png) | 6 DAG-ов, расписание и задачи |
| [05\_data\_flow.puml](docs/diagrams/05_data_flow.puml) | [PNG](docs/diagrams/data_flow.png) | Сквозной поток данных (sequence diagram) |
| [06\_api.puml](docs/diagrams/06_api.puml) | [PNG](docs/diagrams/api.png) | FastAPI: все эндпоинты и источники данных |
| [07\_ducklake\_internals.puml](docs/diagrams/07_ducklake_internals.puml) | [PNG](docs/diagrams/ducklake_internals.png) | Устройство DuckLake: снэпшоты, партиции, ACID, time travel |
| [08\_field\_reference.puml](docs/diagrams/08_field_reference.puml) | [PNG](docs/diagrams/field_reference.png) | Расшифровка каждого поля всех таблиц |

### English docs

| Document | Description |
|----------|-------------|
| [docs/en/QUICKSTART.md](docs/en/QUICKSTART.md) | Step-by-step setup guide |
| [docs/en/ARCHITECTURE.md](docs/en/ARCHITECTURE.md) | Architecture, services, design decisions |
| [docs/en/DATA_MODEL.md](docs/en/DATA_MODEL.md) | Data model, tables, partitioning |
| [docs/en/DBT_LAYERS.md](docs/en/DBT_LAYERS.md) | dbt layers, critical DuckLake fixes |
| [docs/en/DUCKLAKE_FEATURES.md](docs/en/DUCKLAKE_FEATURES.md) | DuckLake features, limitations, Iceberg comparison |
| [docs/en/SCALING.md](docs/en/SCALING.md) | Limitations and scaling paths |

## Лицензия

MIT
