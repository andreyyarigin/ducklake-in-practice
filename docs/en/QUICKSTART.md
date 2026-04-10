# Quick Start — ducklake-in-practice

Step-by-step guide: from cloning the repository to working dashboards.

---

## Requirements

| Tool | Minimum version | Purpose |
|---|---|---|
| Docker | 24+ | All services run in containers |
| Docker Compose | v2.20+ | `docker compose` (not `docker-compose`) |
| RAM | 8 GB+ | DuckDB holds data in memory during dbt transformations |
| Disk | 5 GB+ | Parquet files + Docker images |

---

## Step 1 — Clone and configure

```bash
git clone <repo-url>
cd ducklake-in-practice

# Copy environment config
cp .env.example .env
```

The `.env` file contains passwords for PostgreSQL, MinIO, and Airflow. Defaults are preconfigured for local use — no changes needed.

---

## Step 2 — Start services

```bash
docker compose up -d
```

On first run, Docker downloads images (~3–5 GB) and builds custom Dockerfiles. Expect 5–10 minutes.

### What starts up

| Service | Container | Role |
|---|---|---|
| MinIO | `dl-minio` | S3-compatible Parquet storage |
| PostgreSQL | `dl-postgres` | DuckLake catalog + Airflow metadata + Superset appdb |
| Redis | `dl-redis` | Celery broker for Airflow |
| Airflow Webserver | `dl-airflow-webserver` | UI + REST API |
| Airflow Scheduler | `dl-airflow-scheduler` | DAG scheduler |
| Airflow Worker 1/2 | `dl-airflow-worker-1/2` | Task execution |
| FastAPI | `dl-api` | REST analytics API |
| Superset | `dl-superset` | BI dashboards |

### Check status

```bash
docker compose ps
```

All services should be `healthy`. Airflow and Superset take the longest — wait 2–3 minutes after `docker compose up`.

```bash
# Stream logs
docker compose logs -f airflow-webserver
```

---

## Step 3 — Load seed data

Seeds are loaded **once** during initial setup. This includes airports, airlines, routes (OpenFlights), aircraft types, and route profiles.

```bash
make seeds
```

Or manually:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/load_seeds.py
```

**What gets loaded:**
- `airports` — 177 Russian airports
- `airlines` — ~30 active Russian carriers
- `routes` — ~600 domestic routes
- `aircraft_types` — 15 aircraft types
- `route_profiles` — route profiles (load_factor, price_tier, seasonality)

---

## Step 4 — Generate historical data (backfill)

To have data in the dashboards right away, populate DuckLake for a past period.

```bash
# Backfill the last ~100 days
make backfill FROM=2026-01-01 TO=2026-04-08
```

Or manually:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/backfill.py \
    --from 2026-01-01 --to 2026-04-08
```

> **Runtime:** ~5–15 minutes depending on the date range.

**What gets generated per day:**
- ~800 flights (based on OpenFlights routes)
- Bookings by demand curve (economy / business / first)
- Price history (dynamic pricing)
- Weather for all airports (Open-Meteo API)

---

## Step 5 — Run dbt (transformations)

dbt transforms raw data into analytical marts.

> **Important:** layers run as **separate commands** — this is a required workaround to prevent OOM with 7.8M booking rows.

```bash
# Option 1: via Makefile (runs all layers sequentially)
make dbt-run

# Option 2: manually, layer by layer
docker compose exec airflow-worker-1 bash -c "
  cd /opt/ducklake-in-practice/dbt/ducklake_flights && \
  dbt deps && \
  dbt run --select staging && \
  dbt run --select int_bookings_daily_agg && \
  dbt run --select intermediate && \
  dbt run --select marts
"
```

**What gets created:**

| Layer | Models | Description |
|---|---|---|
| staging | 10 | Cleaning, type casting, filtering |
| intermediate | 4 | Enrichment, denormalization, pre-aggregation |
| marts | 10 | Business metrics, ready for Superset and API |

---

## Step 6 — Export to Serving Store

Superset and FastAPI read from a local DuckDB file (`/serving/flights.duckdb`), not directly from DuckLake. The export creates this file atomically.

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

> **In automated mode:** the export runs automatically via the `maintenance` DAG at 03:00 UTC.

---

## Step 7 — Open the interfaces

| Service | URL | Login / Password |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| FastAPI Swagger | http://localhost:8000/docs | — |
| Superset | http://localhost:8088 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |

### Superset: open the dashboard

1. Go to http://localhost:8088
2. Login: `admin` / `admin`
3. Menu → **Dashboards** → **DuckLake Flights Analytics**

### FastAPI: verify data

```bash
# Top routes by revenue
curl http://localhost:8000/routes/top?limit=10

# Daily metrics for route SVO-LED
curl "http://localhost:8000/routes/SVO-LED/daily?date_from=2026-01-01&date_to=2026-04-01"

# DuckLake snapshots list (time travel)
curl http://localhost:8000/time-travel/snapshots
```

---

## Daily cycle (automated)

After initial setup, Airflow runs all DAGs automatically:

| UTC time | DAG | What it does |
|---|---|---|
| 00:30 | `ingest_flights` | Generates ~800 flights +7 days ahead |
| 00:15, 06:15, 12:15, 18:15 | `ingest_bookings` | Generates bookings (4 batches/day) |
| 01:00 | `ingest_weather` | Loads weather from Open-Meteo |
| 02:00 | `dbt_run_daily` | Recomputes all dbt models |
| 03:00 | `maintenance` | Snapshot cleanup + serving store export |
| 03:30 | `dbt_test_daily` | 83 data quality tests |

---

## Health check

```bash
make health
```

Or manually:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/healthcheck.py
```

Checks: DuckLake ATTACH, row counts in all tables, data freshness (lag > 3h → WARNING).

---

## Full reset (clean start)

```bash
# Delete all data and containers
make clean-volumes

# Start fresh
make init                                         # = docker compose up -d + seeds
make backfill FROM=2026-01-01 TO=2026-04-08
```

> ⚠️ `make clean-volumes` deletes **all** Parquet files, PostgreSQL data, and Redis state. Irreversible.

---

## Troubleshooting

### Airflow DAG won't start
```bash
docker compose logs -f airflow-scheduler
docker compose ps
```

### dbt OOM crash
```bash
# Make sure you're running layers separately, not `dbt run` without --select
# Verify threads=1 in profiles.yml
cat dbt/ducklake_flights/profiles.yml | grep threads
```

### Superset shows no data
```bash
# Check serving store exists
docker compose exec airflow-worker-1 ls -la /serving/

# Re-export manually
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

### DuckLake ATTACH error
```bash
docker compose exec airflow-worker-1 \
    python -c "from src.generators.connection import get_ducklake_connection; conn = get_ducklake_connection(); print('OK')"
```

---

## Next steps

- [ARCHITECTURE.md](ARCHITECTURE.md) — detailed architecture and design decisions
- [DATA_MODEL.md](DATA_MODEL.md) — all tables, fields, partitioning
- [DBT_LAYERS.md](DBT_LAYERS.md) — dbt models, critical DuckLake fixes
- [DUCKLAKE_FEATURES.md](DUCKLAKE_FEATURES.md) — ACID transactions, time travel, comparison with Iceberg
- [SCALING.md](SCALING.md) — limitations and growth paths
