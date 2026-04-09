# ducklake-in-practice Architecture

## Overview

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
  │                  │  │  (Parquet files, date-partitioned)   │  │    │
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

## Services

| Service | Image | Port | Role |
|---------|-------|------|------|
| airflow-webserver | apache/airflow | 8080 | UI, REST API |
| airflow-scheduler | apache/airflow | — | DAG scheduler |
| airflow-worker | apache/airflow | — | CeleryExecutor worker (x2) |
| postgres | postgres:16 | 5433 | Three databases |
| redis | redis:7 | 6379 | Celery broker |
| minio | minio/minio | 9000, 9001 | S3 Parquet storage |
| fastapi | (custom) | 8000 | REST serving |
| superset | apache/superset:4.1.1 | 8088 | BI dashboards (admin/admin) |
| init-serving-store | (custom) | — | Bootstrap serving store on startup |

> **Note:** the `minio` service has the network alias `rustfs` in Docker Compose.
> DuckLake stores file paths with the hostname `rustfs:9000`.
> Do not rename this host or change the alias without recreating all tables.

## Data Flow

```
OpenFlights CSV          Python Generator
(airports, airlines,     (flights, bookings,
 routes)                  passengers, price_history)
      │                          │
      ▼                          ▼
  load_seeds.py           ingest_flights DAG
      │                    (daily)
      └──────────┬──────────────┘
                 ▼
       DuckLake INSERT
       (raw tables in flights.main)
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

## Key Architectural Decisions

### DuckLake as the single source of truth

All data lives only in DuckLake. The serving store (`/serving/flights.duckdb`) is derived from the mart layer and refreshed on a schedule by an Airflow DAG.

**Alternative we rejected:** writing aggregates to a separate PostgreSQL database. This creates duplication, synchronization issues, and additional ETL.

### Serving store is the right pattern, not a limitation

DuckLake is not a serving layer by design: reading Parquet files from S3 via the DuckLake extension on every BI request incurs too much network I/O latency. The correct pattern is:

```
DuckLake (storage + ACID + transforms via dbt)
    ↓  dag_export_serving_store (Airflow, 03:00 UTC)
    ↓  atomic: write flights_new.duckdb → rename to flights.duckdb
serving store: /serving/flights.duckdb
    ↓  duckdb:////serving/flights.duckdb
Superset (BI)  +  FastAPI (REST API)
```

This is the standard export-to-serving pattern used in production lakehouses: Iceberg→Redshift, Delta→Synapse, DuckLake→DuckDB file. Superset connects via the SQLAlchemy URI `duckdb:////serving/flights.duckdb` — a standard DuckDB file, no extensions required.

### dbt pre-aggregates, serving only reads

FastAPI and Superset never perform complex join queries. All joins run inside dbt models. Serving reads only mart tables with pre-computed aggregates.

**Why:** not because of DuckDB's join capability (it handles in-memory joins extremely well), but because of I/O latency: reading Parquet files from S3 over the network is significantly slower than working with local data. By pre-aggregating in dbt, we minimize the data volume that serving needs to read per request.

### One PostgreSQL — three databases

A single PostgreSQL instance hosts:
- `ducklake_catalog` — DuckLake metadata (tables, partitions, snapshots, files)
- `airflow_metadata` — DAG state, tasks, Airflow logs
- `superset_appdb` — Superset settings and dashboards

**Trade-off:** resource savings in the sandbox. In production each system deserves its own PostgreSQL.

### Partitioning by flight_date

All transactional tables are partitioned by `flight_date`. This enables:
- DuckLake partition pruning to read only the required Parquet files
- DuckLake to read only the necessary Parquet files (partition pruning)
- Efficient expire/cleanup for the raw layer (TTL = 7 days)

## First-Run Lessons (Critical Fixes)

The following issues were discovered during the first run. All are documented and fixed.

### 1. dbt profiles.yml: DATA_PATH not supported in options

**Problem:** the standard `attach.options.DATA_PATH` syntax does not work with DuckLake in dbt-duckdb.

**Symptom:** dbt crashes when trying to ATTACH with DATA_PATH in the options block.

**Fix:** a custom plugin `dbt/plugins/ducklake_attach_plugin.py` that performs ATTACH via direct SQL with the correct syntax before the dbt session starts.

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

### 2. DuckLake does not support CASCADE DROP

**Problem:** dbt executes `DROP TABLE ... CASCADE` when recreating a table. DuckLake returns an error.

**Fix:** an overriding macro `dbt/macros/drop_relation.sql` that performs DROP without CASCADE.

```sql
-- dbt/macros/drop_relation.sql
{% macro drop_relation(relation) %}
  {% call statement('drop_relation') %}
    DROP {{ relation.type }} IF EXISTS {{ relation }}
  {% endcall %}
{% endmacro %}
```

### 3. threads: 1 is required

**Problem:** DuckLake does not support concurrent writes from multiple threads within a single dbt process.

**Symptom:** with `threads: 4`, models fail with locking or transaction conflict errors.

**Fix:** always set `threads: 1` in `profiles.yml` for the DuckLake profile.

### 4. staging must be materialized: table (not view)

**Problem:** dbt opens new DuckDB connections between pipeline steps. In-memory views do not persist across connections.

**Symptom:** intermediate models cannot see stg_* tables.

**Fix:** all staging models use `materialized: table` in `flights.main`. Physical tables are accessible in any connection.

### 5. OOM when running the full pipeline in one command

**Problem:** a single `dbt run` over all 25 models causes OOM with 7.8M booking rows — DuckDB keeps previous models in memory while building subsequent ones.

**Fix:** the Airflow DAG runs each layer as a separate command:
```
dbt run --select staging
dbt run --select int_bookings_daily_agg
dbt run --select intermediate
dbt run --select marts
```

The `int_bookings_daily_agg` model pre-aggregates 7.8M bookings → ~71K rows before mart joins.

### 6. MinIO: rustfs network alias is required

**Problem:** DuckLake stores Parquet file paths in the PostgreSQL catalog using the hostname from the time of writing. If data was written through `rustfs:9000`, it will always be read through `rustfs:9000`.

**Fix:** the MinIO service in Docker Compose has the alias `rustfs`. Never change this alias or rename the host without recreating all tables.

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

### 7. Serving store: atomic export of mart tables

**Problem:** DuckLake is not a serving layer — reading Parquet files from S3 via the DuckLake extension on every BI request is too slow due to network I/O latency.

**Fix:** `docker/export-serving-store.py` exports mart tables from DuckLake into a local DuckDB file atomically: it writes `flights_new.duckdb` first, then `shutil.move()` → `flights.duckdb`. Superset and FastAPI read the ready-made file through standard DuckDB with no extensions required.

```
DuckLake → export-serving-store.py → flights_new.duckdb → (rename) → flights.duckdb
```

The Airflow DAG `dag_export_serving_store` runs daily at 03:00 UTC, after `dbt_run_daily` (02:00 UTC). On first startup, the `init-serving-store` service creates the initial file.

### 8. dbt deps must run before every dbt run

**Problem:** on container startup, `dbt_packages/` is empty — packages (dbt_utils) are not installed. `dbt run` fails with:
```
dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed
```

**Fix:** the Airflow DAG `dbt_run_daily` runs `dbt deps` as the first task before `dbt_run_staging`. Pipeline order: `dbt_deps → dbt_run_staging → dbt_run_bookings_agg → dbt_run_intermediate → dbt_run_marts`.

### 9. `changes()` is a SQLite function — not available in DuckDB

**Problem:** `SELECT changes()` was used after UPDATE to get the affected row count. DuckDB does not have this function (it is SQLite-only):
```
CatalogError: Scalar Function with name changes does not exist!
```

**Fix:** replace with an explicit `SELECT COUNT(*)` filtered on updated rows.

```python
# Wrong (SQLite-only):
conn.execute("SELECT changes()").fetchone()[0]

# Correct (DuckDB):
conn.execute(
    "SELECT COUNT(*) FROM flights.flights WHERE flight_date = ? AND status = 'arrived' AND updated_at >= ?",
    [yesterday, now]
).fetchone()[0]
```

Affected: `dag_ingest_flights.py` (task `update_flight_statuses`), `dag_ingest_weather.py` (task `fetch_and_store_weather`).

### 10. Naive vs aware datetime when subtracting via pendulum

**Problem:** DuckDB returns TIMESTAMP columns as timezone-naive Python `datetime`. Subtracting from `datetime.now(timezone.utc)` (timezone-aware) raises:
```
TypeError: can't compare offset-naive and offset-aware datetimes
```

**Fix:** normalize before arithmetic.

```python
if departure.tzinfo is None:
    departure = departure.replace(tzinfo=timezone.utc)
days_total = (departure - now).days
```

Affected: `src/generators/price_generator.py`, called from `dag_ingest_bookings.py`.

### 11. Superset: SQLAlchemy URI for the DuckDB file

**Problem:** Superset needs to connect to the serving store DuckDB file without DuckLake extensions.

**Fix:** Superset uses the SQLAlchemy URI `duckdb:////serving/flights.duckdb` via `duckdb-engine`. The `docker/Dockerfile.superset` image is based on `apache/superset:4.1.1` and installs `duckdb==1.3.0 + duckdb-engine + psycopg2-binary`. The Superset configuration (`docker/superset_config.py`) connects PostgreSQL (`superset_appdb`) as the metadata DB and Redis (DB 1) as the cache.

The final dashboard contains **13 charts** organized in 6 thematic sections:

| Section | Charts |
|---------|--------|
| Operational metrics | Top-10 routes by revenue; Top-15 routes by passengers; Top-10 airlines: flights and cancellations |
| Prices and passenger segments | Top-10 routes by average ticket price; Average price by class and booking horizon; Passenger segments (pie) |
| Airports and carrier reliability | Airport traffic top-10 + others (pie); Delays and cancellations by airline % of flights (stacked bar, excluding "on-time") |
| Routes with highest cancellation rate | Top-10 routes by cancelled flight share |
| Delays by airline | Airlines with the most delays top-10; Airlines with the fewest delays top-10 |
| Delays by route | Routes with the most delays top-10; Routes with the fewest delays top-10 |

**Key finding:** route is the dominant delay factor (3–5× spread); the airline factor is minimal (~2–3 min). Seasonality has no effect.

## Network and Service Interaction

All services are in a single Docker network `default`. Service names act as DNS names.

| Connection | Mechanism |
|-----------|-----------|
| Workers → PostgreSQL | psycopg2 / DuckDB postgres extension |
| Workers → MinIO | DuckDB httpfs + S3 secret |
| Workers → DuckLake | DuckDB ducklake extension (ATTACH) |
| FastAPI → DuckLake | DuckDB in-process (read-only) |
| export-serving-store → DuckLake | DuckDB in-process (read-only) |
| Superset → serving store | duckdb-engine (SQLAlchemy, file path) |
| FastAPI → serving store | DuckDB in-process (file path) |
| Airflow Scheduler → Workers | Redis (Celery) |
