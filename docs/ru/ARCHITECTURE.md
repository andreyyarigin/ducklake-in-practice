# Архитектура ducklake-in-practice

## Общая схема

![Архитектура](../diagrams/architecture.png)

<details>
<summary>ASCII-версия</summary>

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                        Docker Compose                               │
  │                                                                     │
  │  ┌─────────────┐      ┌──────────────────────────────────────────┐ │
  │  │  Airflow    │      │            Airflow Workers                │ │
  │  │  Webserver  │─────>│  worker-1           worker-2             │ │
  │  │  :8080      │      │  (ingestion DAGs)   (dbt DAGs)           │ │
  │  │  Scheduler  │      └──────────────────────────────────────────┘ │
  │  └─────────────┘             │ ingest              │ dbt run        │
  │         │                    ▼                     ▼               │
  │  ┌──────┴───┐    ┌───────────────────────────────────────────┐    │
  │  │  Redis   │    │               DuckLake layer               │    │
  │  │ (broker) │    │                                            │    │
  │  └──────────┘    │  ┌─────────────────────────────────────┐  │    │
  │                  │  │     PostgreSQL :5433                  │  │    │
  │                  │  │  ┌──────────────────────────────┐    │  │    │
  │                  │  │  │  ducklake_catalog (DuckLake)  │    │  │    │
  │                  │  │  │  airflow_metadata             │    │  │    │
  │                  │  │  │  superset_appdb               │    │  │    │
  │                  │  │  └──────────────────────────────┘    │  │    │
  │                  │  └─────────────────────────────────────┘  │    │
  │                  │                                            │    │
  │                  │  ┌─────────────────────────────────────┐  │    │
  │                  │  │     MinIO / rustfs :9000             │  │    │
  │                  │  │  s3://ducklake-flights/data/         │  │    │
  │                  │  │  (Parquet files, партиции по дате)   │  │    │
  │                  │  └─────────────────────────────────────┘  │    │
  │                  └───────────────────────────────────────────┘    │
  │                            │ dag_export_serving_store (03:00 UTC)  │
  │                            ▼                                       │
  │                  ┌─────────────────────────────────────────────┐   │
  │                  │  serving store: /serving/flights.duckdb      │   │
  │                  │  (atomic: write flights_new.duckdb → rename) │   │
  │                  └──────────────────┬──────────────────────────┘   │
  │          ┌───────────────────────────┘                             │
  │          ▼                           ▼                             │
  │  ┌──────────────┐        ┌──────────────────────┐                 │
  │  │   FastAPI    │        │      Superset         │                 │
  │  │   :8000      │        │      :8088            │                 │
  │  │  DuckDB      │        │  duckdb-engine        │                 │
  │  │  in-process  │        │  /serving/flights.duckdb               │
  │  └──────────────┘        └──────────────────────┘                 │
  └────────────────────────────────────────────────────────────────────┘
```

</details>

## Сервисы

| Сервис | Образ | Порт | Роль |
|--------|-------|------|------|
| airflow-webserver | apache/airflow | 8080 | UI, REST API |
| airflow-scheduler | apache/airflow | — | Планировщик DAG |
| airflow-worker | apache/airflow | — | CeleryExecutor worker (x2) |
| postgres | postgres:16 | 5433 | Три базы данных |
| redis | redis:7 | 6379 | Celery broker |
| minio | minio/minio | 9000, 9001 | S3-хранилище Parquet |
| fastapi | (custom) | 8000 | REST serving |
| superset | apache/superset:4.1.1 | 8088 | BI-дашборды (admin/admin) |
| init-serving-store | (custom) | — | Bootstrap serving store при старте |

> **Заметка:** сервис `minio` имеет сетевой alias `rustfs` в Docker Compose.
> DuckLake сохраняет пути к файлам с именем хоста `rustfs:9000`.
> Не переименовывать хост и не менять alias без пересоздания таблиц.

## Поток данных

![Поток данных](../diagrams/data_flow.png)

<details>
<summary>ASCII-версия</summary>

```
OpenFlights CSV          Python Generator
(airports, airlines,     (flights, bookings,
 routes)                  passengers, price_history)
      │                          │
      ▼                          ▼
  load_seeds.py           ingest_flights DAG
      │                    (ежедневно)
      └──────────┬──────────────┘
                 ▼
       DuckLake INSERT
       (raw tables в flights.main)
                 │
                 ▼
          dbt run staging
          (table, flights.main)
                 │
                 ▼
       dbt run intermediate
       (materialized: table)
                 │
                 ▼
          dbt run marts
       (materialized: table)
                 │
                 ▼
     dag_export_serving_store
     (Airflow, 03:00 UTC)
     export-serving-store.py:
     flights_new.duckdb → flights.duckdb
                 │
         ┌───────┴────────┐
         ▼                ▼
     FastAPI          Superset
   (DuckDB            (duckdb-engine,
   in-process)         /serving/flights.duckdb)
```

</details>

## Принятые архитектурные решения

### DuckLake как единственный source of truth

Все данные живут только в DuckLake. Serving store (`/serving/flights.duckdb`) — производная от mart-слоя, обновляемая по расписанию Airflow DAG.

**Альтернатива которую мы отвергли:** писать агрегаты в отдельную PostgreSQL-базу. Это создаёт дублирование, синхронизацию и дополнительный ETL.

### Serving store — правильный паттерн, не ограничение

DuckLake не является serving-слоем по дизайну: читать Parquet через DuckLake-расширение для каждого запроса Superset — слишком много I/O latency. Правильный паттерн:

```
DuckLake (хранение + ACID + трансформации через dbt)
    ↓  dag_export_serving_store (Airflow, 03:00 UTC)
    ↓  атомарно: запись flights_new.duckdb → переименование в flights.duckdb
serving store: /serving/flights.duckdb
    ↓  duckdb:////serving/flights.duckdb
Superset (BI)  +  FastAPI (REST API)
```

Это стандартный export-to-serving паттерн промышленных lakehouse: Iceberg→Redshift, Delta→Synapse, DuckLake→DuckDB файл. Superset подключается через SQLAlchemy URI `duckdb:////serving/flights.duckdb` — стандартный DuckDB-файл, никаких расширений не нужно.

### dbt агрегирует заранее, serving только читает

FastAPI и Superset никогда не делают сложных join-запросов. Все join-ы выполняются в dbt-моделях. Serving читает только mart-таблицы с готовыми агрегатами.

**Почему:** не из-за ограничений DuckDB как движка (он отлично делает join в памяти), а из-за I/O latency: читать Parquet-файлы из S3 по сети заметно медленнее, чем работать с локальными данными. Агрегируя заранее в dbt, мы минимизируем объём данных, которые serving должен читать при каждом запросе.

### Один PostgreSQL — три базы

Один инстанс PostgreSQL держит:
- `ducklake_catalog` — метаданные DuckLake (таблицы, партиции, снэпшоты, файлы)
- `airflow_metadata` — состояние DAG, задачи, логи Airflow
- `superset_appdb` — настройки и дашборды Superset

**Компромисс:** экономия ресурсов в sandbox. В production каждая система заслуживает своего PostgreSQL.

### Partitioning по flight_date

Все транзакционные таблицы партиционированы по `flight_date`. Это позволяет:
- dbt читать только нужные Parquet-файлы (partition pruning)
- DuckLake читать только нужные Parquet-файлы (partition pruning)
- Эффективный expire/cleanup для raw-слоя (TTL = 7 дней)

## Уроки первого запуска (критические фиксы)

Ниже — проблемы, обнаруженные при первом запуске. Все они задокументированы и исправлены.

### 1. dbt profiles.yml: DATA_PATH не поддерживается в options

**Проблема:** стандартный синтаксис `attach.options.DATA_PATH` не работает с DuckLake в dbt-duckdb.

**Симптом:** dbt падает при попытке ATTACH с параметром DATA_PATH в блоке options.

**Решение:** кастомный плагин `dbt/plugins/ducklake_attach_plugin.py`, который выполняет ATTACH через прямой SQL с нужным синтаксисом до начала dbt-сессии.

```python
# dbt/plugins/ducklake_attach_plugin.py
def connect(self, config):
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake; ...")
    conn.execute("""
        ATTACH 'ducklake:postgres:host=postgres ...'
        AS flights (DATA_PATH 's3://ducklake-flights/data/')
    """)
    return conn
```

### 2. DuckLake не поддерживает CASCADE DROP

**Проблема:** dbt при пересоздании таблицы выполняет `DROP TABLE ... CASCADE`. DuckLake возвращает ошибку.

**Решение:** переопределяющий макрос `dbt/macros/drop_relation.sql`, который выполняет DROP без CASCADE.

```sql
-- dbt/macros/drop_relation.sql
{% macro drop_relation(relation) %}
  {% call statement('drop_relation') %}
    DROP {{ relation.type }} IF EXISTS {{ relation }}
  {% endcall %}
{% endmacro %}
```

### 3. threads: 1 обязателен

**Проблема:** DuckLake не поддерживает конкурентную запись из нескольких потоков одного dbt-процесса.

**Симптом:** при `threads: 4` модели падают с ошибками блокировки или конфликта транзакций.

**Решение:** в `profiles.yml` всегда `threads: 1` для DuckLake-профиля.

### 4. staging должен быть materialized: table (не view)

**Проблема:** dbt открывает новые DuckDB-соединения между шагами pipeline. Views в памяти не переносятся между соединениями.

**Симптом:** intermediate-модели не видят stg_* таблицы.

**Решение:** все staging-модели — `materialized: table` в `flights.main`. Физические таблицы доступны в любом соединении.

### 5. OOM при полном pipeline в одной команде

**Проблема:** `dbt run` всех 25 моделей в одном процессе — OOM при 7.8M строк бронирований. DuckDB держит предыдущие модели в памяти пока запускает следующие.

**Решение:** Airflow DAG запускает слои отдельными командами:
```
dbt run --select staging
dbt run --select int_bookings_daily_agg
dbt run --select intermediate
dbt run --select marts
```

Промежуточные результаты — предагрегация в `int_bookings_daily_agg`: 7.8M → ~71K строк.

### 6. MinIO: network alias rustfs обязателен

**Проблема:** DuckLake сохраняет пути к Parquet-файлам в каталог PostgreSQL с именем хоста из момента записи. Если данные записаны через хост `rustfs:9000`, они всегда будут читаться через `rustfs:9000`.

**Решение:** сервис MinIO в Docker Compose имеет alias `rustfs`. Никогда не менять этот alias и не переименовывать хост без пересоздания всех таблиц.

```yaml
# docker-compose.yml
services:
  minio:
    image: minio/minio
    networks:
      default:
        aliases:
          - rustfs
```

### 7. Serving store: атомарный экспорт mart-таблиц

**Проблема:** DuckLake не является serving-слоем — читать Parquet-файлы из S3 через DuckLake-расширение при каждом запросе BI-инструмента слишком медленно из-за сетевого I/O.

**Решение:** `docker/export-serving-store.py` экспортирует mart-таблицы из DuckLake в локальный DuckDB-файл атомарно: сначала записывает `flights_new.duckdb`, затем `shutil.move()` → `flights.duckdb`. Superset и FastAPI читают готовый файл через стандартный DuckDB без каких-либо расширений.

```
DuckLake → export-serving-store.py → flights_new.duckdb → (rename) → flights.duckdb
```

Airflow DAG `dag_export_serving_store` запускается ежедневно в 03:00 UTC после `dbt_run_daily` (02:00 UTC). При первом старте сервис `init-serving-store` создаёт начальный файл.

### 8. dbt deps обязателен перед каждым dbt run

**Проблема:** при первом старте контейнеров папка `dbt_packages/` пуста — пакеты (dbt_utils) не установлены. `dbt run` падает:
```
dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed
```

**Решение:** Airflow DAG `dbt_run_daily` запускает `dbt deps` как первый шаг перед `dbt_run_staging`. Шаги: `dbt_deps → dbt_run_staging → dbt_run_bookings_agg → dbt_run_intermediate → dbt_run_marts`.

```python
deps = PythonOperator(task_id="dbt_deps", python_callable=_run_dbt, op_args=[["deps"]])
deps >> run_staging >> run_bookings_agg >> run_intermediate >> run_marts
```

### 9. `changes()` — функция SQLite, не DuckDB

**Проблема:** в DAGs использовалась `SELECT changes()` после UPDATE для получения числа затронутых строк. В DuckDB эта функция не существует (это SQLite API).

```
CatalogError: Scalar Function with name changes does not exist!
```

**Решение:** заменить на явный `SELECT COUNT(*)` с фильтром по обновлённым строкам.

```python
# Неправильно (SQLite-only):
conn.execute("SELECT changes()").fetchone()[0]

# Правильно (DuckDB):
conn.execute(
    "SELECT COUNT(*) FROM flights.flights WHERE flight_date = ? AND status = 'arrived' AND updated_at >= ?",
    [yesterday, now]
).fetchone()[0]
```

Затронуло: `dag_ingest_flights.py` (task `update_flight_statuses`), `dag_ingest_weather.py` (task `fetch_and_store_weather`).

### 10. Naive vs aware datetime при сравнении через pendulum

**Проблема:** DuckDB возвращает TIMESTAMP-колонки как Python `datetime` без timezone (naive). При вычитании naive и aware datetime через pendulum возникает ошибка:

```
TypeError: can't compare offset-naive and offset-aware datetimes
```

**Симптом:** `price_generator.py` вычислял `(departure - now).days`, где `departure` — из DuckLake (naive), `now = datetime.now(timezone.utc)` (aware).

**Решение:** нормализовать `departure` перед арифметикой.

```python
if departure.tzinfo is None:
    departure = departure.replace(tzinfo=timezone.utc)
days_total = (departure - now).days
```

Затронуло: `src/generators/price_generator.py`, вызывалось из `dag_ingest_bookings.py`.

### 11. Superset: SQLAlchemy URI для DuckDB файла

**Проблема:** Superset должен подключаться к DuckDB-файлу serving store без DuckLake-расширений.

**Решение:** Superset использует SQLAlchemy URI `duckdb:////serving/flights.duckdb` через `duckdb-engine`. Образ `docker/Dockerfile.superset` на базе `apache/superset:4.1.1` устанавливает `duckdb==1.3.0 + duckdb-engine + psycopg2-binary`. Конфигурация Superset (`docker/superset_config.py`) подключает PostgreSQL (`superset_appdb`) как metadata DB и Redis (DB 1) как кэш.

Финальный дашборд содержит **13 чартов**, организованных в 6 тематических секций:

| Секция | Чарты |
|--------|-------|
| Операционные показатели | Топ-10 направлений по выручке; Топ-15 направлений по пассажирам; Топ-10 авиакомпаний: рейсы и отмены |
| Цены и сегменты пассажиров | Топ-10 направлений по средней цене билета; Средняя цена по классу и горизонту бронирования; Сегменты пассажиров (pie) |
| Аэропорты и надёжность перевозчиков | Трафик аэропортов топ-10 + остальные (pie); Задержки и отмены по авиакомпаниям % рейсов (stacked bar, без "вовремя") |
| Маршруты с наибольшей долей отмен | Топ-10 направлений по доле отменённых рейсов |
| Задержки по авиакомпаниям | Авиакомпании с наибольшими задержками топ-10; Авиакомпании с наименьшими задержками топ-10 |
| Задержки по маршрутам | Маршруты с наибольшими задержками топ-10; Маршруты с наименьшими задержками топ-10 |

**Ключевой вывод:** маршрут — главный фактор задержек (разброс 3–5×), airline-фактор минимален (~2–3 мин). Сезонность не влияет.

## Сеть и взаимодействие сервисов

Все сервисы находятся в одной Docker-сети `default`. Имена сервисов работают как DNS-имена.

| Соединение | Механизм |
|-----------|---------|
| Workers → PostgreSQL | psycopg2 / DuckDB postgres extension |
| Workers → MinIO | DuckDB httpfs + S3 secret |
| Workers → DuckLake | DuckDB ducklake extension (ATTACH) |
| FastAPI → DuckLake | DuckDB in-process (read-only) |
| export-serving-store → DuckLake | DuckDB in-process (read-only) |
| Superset → serving store | duckdb-engine (SQLAlchemy, file path) |
| FastAPI → serving store | DuckDB in-process (file path) |
| Airflow Scheduler → Workers | Redis (Celery) |
