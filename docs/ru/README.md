# ducklake-in-practice

**Production-grade lakehouse sandbox на DuckDB + DuckLake**

Аналитическая платформа авиабронирований внутренних рейсов РФ, демонстрирующая возможности и ограничения экосистемы DuckDB для построения полноценного lakehouse.

## Цель проекта

Ответить на вопрос: **как выглядит взрослый lakehouse, построенный на экосистеме DuckDB?**

Проект намеренно использует production-паттерны при небольшом объёме данных (~500 МБ/день). Архитектурные решения принимаются так, как если бы данных было на порядки больше. Все ограничения и обходные пути задокументированы честно.

## Что внутри

- **~99 200 рейсов**, **~7.8M бронирований**, **~213 600 записей истории цен** и **~17 000 погодных наблюдений** на синтетических данных внутренних рейсов РФ
- Полный lakehouse-стек: ingestion → staging → intermediate → marts → serving
- Обнаруженные и задокументированные подводные камни DuckLake первого запуска

## Стек

| Компонент | Технология | Роль |
|-----------|-----------|------|
| Object storage | MinIO | S3-совместимое хранилище Parquet-файлов |
| Metadata catalog | PostgreSQL | Каталог DuckLake + метаданные Airflow + Superset appdb |
| Lakehouse format | DuckLake | ACID, time travel, schema evolution, partitioning |
| Compute engine | DuckDB | Запись, трансформации, чтение — in-process |
| Orchestration | Airflow (CeleryExecutor) | DAGs для ingestion, трансформаций и экспорта |
| Message broker | Redis | Broker для Celery |
| Transformations | dbt (dbt-duckdb) | staging → intermediate → marts |
| BI | Apache Superset | Дашборды поверх serving store |
| API | FastAPI | REST-аналитика, DuckDB in-process |

## Архитектура

![Общая архитектура](../diagrams/architecture.png)

> Полная диаграмма: [`docs/diagrams/01_architecture.puml`](../diagrams/01_architecture.puml)

## Поток данных

![Сквозной поток данных](../diagrams/data_flow.png)

> Полная диаграмма: [`docs/diagrams/05_data_flow.puml`](../diagrams/05_data_flow.puml)

## Ключевые архитектурные решения

1. **DuckLake — единственный source of truth.** Все данные живут в DuckLake. Serving store — производная, обновляемая по расписанию.
2. **dbt агрегирует заранее.** Тяжёлый compute — в dbt по расписанию. Serving только читает готовые marts.
3. **Serving store — правильный паттерн, не workaround.** Экспорт mart-таблиц из DuckLake в `flights.duckdb` — это стандартный export-to-serving паттерн (аналог Iceberg→Redshift, Delta→Synapse). Superset и FastAPI читают лёгкий файл без DuckLake-расширений.
4. **Один PostgreSQL — три базы:** `ducklake_catalog`, `airflow_metadata`, `superset_appdb`.
5. **Данные: OpenFlights seed + синтетическая генерация.** Никаких внешних API. ~500 МБ Parquet/день.
6. **Только внутренние рейсы РФ.** Масштабирование через расширение seed.

## Быстрый старт

```bash
# 1. Клонировать и настроить
git clone <repo-url> && cd ducklake-in-practice
cp .env.example .env

# 2. Поднять сервисы и загрузить справочники
docker compose up -d
make seeds                                        # аэропорты, авиакомпании, маршруты, типы ВС

# 3. Наполнить данными
make backfill FROM=2026-01-01 TO=2026-04-08      # генерация рейсов, бронирований, погоды

# 4. Трансформировать (dbt слои раздельно — иначе OOM)
make dbt-run

# 5. Экспортировать в serving store
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

> Подробный гайд с разбором каждого шага, troubleshooting и описанием всех команд: **[QUICKSTART.md](QUICKSTART.md)**

## Интерфейсы

| Сервис | URL | Credentials |
|--------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| FastAPI docs | http://localhost:8000/docs | — |
| Superset | http://localhost:8088 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5433 | ducklake / (см. .env) |

## Документация

| Документ | Описание |
|----------|----------|
| [QUICKSTART.md](QUICKSTART.md) | Пошаговый гайд запуска: от клонирования до дашбордов |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Полная архитектура, сервисы, принятые решения, уроки первого запуска |
| [DATA_MODEL.md](DATA_MODEL.md) | Модель данных, таблицы, колонки, паттерны генерации |
| [DBT_LAYERS.md](DBT_LAYERS.md) | Слои dbt, все модели, материализации, критические фиксы |
| [DUCKLAKE_FEATURES.md](DUCKLAKE_FEATURES.md) | Фичи DuckLake на конкретных примерах, ограничения, gotchas |
| [SCALING.md](SCALING.md) | Текущие ограничения, пути масштабирования, матрица решений |

### Диаграммы

| Диаграмма | Описание |
|-----------|----------|
| [01 — Архитектура](../diagrams/01_architecture.puml) | Сервисы, сеть, взаимодействие компонентов |
| [02 — Модель данных](../diagrams/02_data_model.puml) | Таблицы DuckLake, поля, связи, партиционирование |
| [03 — dbt Lineage](../diagrams/03_dbt_lineage.puml) | Граф зависимостей всех dbt-моделей |
| [04 — Airflow DAGs](../diagrams/04_airflow_dags.puml) | 6 DAGs, расписание и задачи |
| [05 — Поток данных](../diagrams/05_data_flow.puml) | Сквозной sequence diagram от генерации до BI |
| [06 — FastAPI](../diagrams/06_api.puml) | Все эндпоинты, параметры, источники данных |
| [07 — DuckLake Internals](../diagrams/07_ducklake_internals.puml) | Снэпшоты, партиции, ACID, time travel |

Английская версия: [../en/README.md](../en/README.md)

## Лицензия

MIT
