# DuckLake: Features and Limitations

## What are DuckDB and DuckLake

**DuckDB** is an analytical SQL engine. It runs in-process (inside Python, Java, or any application). Think of it as SQLite for analytics. Single process, single machine, no separate server.

**DuckLake** is a table format, analogous to Apache Iceberg or Delta Lake. It does not execute queries. It stores data as Parquet files in object storage (MinIO/S3) and metadata in a SQL database (PostgreSQL). Despite the "Duck" in the name, DuckLake is not tied to DuckDB.

**Analogy:** DuckDB is the aircraft engine. DuckLake is the airport with the hangar and control tower.

## Demonstrated Features

### 1. Time Travel

**Scenario:** an analyst wants to compare average prices on the SVO→LED route over the past week.

```sql
-- List snapshots
SELECT * FROM ducklake_snapshots('flights');

-- Data at snapshot #42
SELECT * FROM flights.price_history
AT SNAPSHOT 42
WHERE flight_id IN (
    SELECT flight_id FROM flights.flights AT SNAPSHOT 42
    WHERE src_airport_iata = 'SVO' AND dst_airport_iata = 'LED'
);

-- Changes between snapshots
SELECT * FROM ducklake_table_changes('flights', 'bookings', 40, 50);
```

**What it demonstrates:** DuckLake stores all data versions without copying Parquet files. Old snapshots reference the same files. Time travel does not cost additional disk space.

### 2. ACID Multi-Table Transactions

**Scenario:** a booking DAG atomically writes to bookings + passengers + price_history.

```sql
BEGIN;
INSERT INTO flights.bookings VALUES (...);
INSERT INTO flights.passengers VALUES (...);
INSERT INTO flights.price_history VALUES (...);
COMMIT;
```

**What it demonstrates:** when two Airflow workers write concurrently, DuckLake resolves conflicts via optimistic concurrency control. If both workers append different data, both commits succeed. If they write to the same row, one rolls back and retries.

**Note:** DuckLake provides atomicity and isolation, but does not fully solve the concurrent writers problem. Under high contention, retry with exponential backoff is needed.

### 3. Schema Evolution

**Scenario:** one month later, we add a `baggage_weight` column to bookings.

```sql
ALTER TABLE flights.bookings ADD COLUMN baggage_weight DECIMAL(5,1);
```

**What it demonstrates:** old Parquet files are not rewritten. DuckLake adds the column to the catalog; new files contain it, old ones do not (read as NULL). Time travel on old snapshots returns data without that column — as if it never existed.

### 4. Partitioning by flight_date

**Scenario:** the daily dbt run processes route data for specific dates.

```sql
CREATE TABLE flights.flights (
    flight_id VARCHAR,
    ...
    flight_date DATE
) PARTITION BY (flight_date);
```

**What it demonstrates:** DuckLake organizes Parquet files by partition. A query with a `flight_date` filter reads only the relevant files (partition pruning). dbt models recalculating a specific route for a period do not touch data from other dates.

### 5. Data Inlining

**Scenario:** booking batches (4 times per day) may be too small for a dedicated Parquet file.

```sql
-- DATA_INLINING_ROW_LIMIT is enabled by default
-- Small batches are stored in the PG catalog, avoiding tiny files
ATTACH 'ducklake:...' AS flights (
    DATA_INLINING_ROW_LIMIT 500
);
```

**What it demonstrates:** small batches do not create a "small file problem". Data is inlined into the PostgreSQL catalog. When enough volume accumulates or on a schedule, it is compacted into Parquet.

### 6. File Compaction

**Scenario:** after several days of batches, small files have accumulated in one partition.

```sql
-- Merge small files
CALL ducklake_merge_adjacent_files('flights');
```

**What it demonstrates:** the operation is fully online and does not block readers. Old files are marked for deletion but are not physically removed until cleanup is called. Compaction improves performance of subsequent reads.

### 7. Snapshot Expiry + Cleanup (raw TTL = 7 days)

**Scenario:** the raw layer is kept for 7 days; old data is removed.

```sql
-- Remove snapshots older than 7 days
CALL ducklake_expire_snapshots('flights', older_than => now() - INTERVAL 7 DAY);

-- Delete files not referenced by any active snapshot
CALL ducklake_cleanup_old_files('flights');
```

**What it demonstrates:** data lifecycle management. Raw layer TTL is 7 days; mart layer is retained indefinitely. Cleanup physically deletes Parquet files from MinIO.

### 8. DuckDB In-Process for API Serving

**Scenario:** FastAPI serves aggregates through DuckDB without a separate query server.

```python
import duckdb

conn = duckdb.connect(read_only=True)
conn.execute("INSTALL ducklake; LOAD ducklake; ...")
conn.execute("ATTACH 'ducklake:postgres:...' AS flights (DATA_PATH 's3://...')")

result = conn.execute("""
    SELECT * FROM flights.mart_route_daily
    WHERE route_key = 'SVO-LED'
    ORDER BY flight_date DESC
    LIMIT 30
""").fetchdf()
```

**What it demonstrates:** DuckDB as an embedded query engine for the API. Reads pre-computed aggregates from the mart layer. No ETL into a separate serving database.

## Known Limitations (as of v0.4)

| Limitation | Impact | Workaround |
|-----------|--------|-----------|
| Experimental status | Not for production with SLA | Sandbox project, acceptable |
| Single-writer per process | Worker contention | Retry with exponential backoff |
| No CASCADE DROP | dbt fails without patch | drop_relation.sql macro |
| No constraints/indexes | Cannot enforce PK/FK at DuckLake level | Validation via dbt tests |
| threads: 1 required | dbt runs slower | Accepted trade-off |
| DATA_PATH not via options | More complex profiles.yml | Custom plugin |
| BI tools via DuckLake directly | High I/O latency for serving queries | Serving store pattern (export-serving-store.py) |
| Join performance on Parquet | Complex joins are slower | Denormalization in mart layer |
| No built-in encryption | Parquet data is unencrypted | Network-level security |

## First-Run Production Gotchas

These are not documentation mistakes — these are real problems discovered during the project's first run.

### executemany() does not work with DuckLake

DuckLake does not support `executemany()`. Attempting batch inserts via `executemany` causes errors or silently inserts nothing.

**Correct approach:** temp table + INSERT SELECT.

```python
# Wrong:
conn.executemany("INSERT INTO flights.bookings VALUES (?, ?, ...)", rows)

# Correct:
conn.execute("CREATE TEMP TABLE tmp_bookings AS SELECT * FROM bookings WHERE 1=0")
conn.executemany("INSERT INTO tmp_bookings VALUES (?, ?, ...)", rows)
conn.execute("INSERT INTO flights.bookings SELECT * FROM tmp_bookings")
```

### `changes()` is a SQLite function — not available in DuckDB

After an UPDATE, DuckDB does not provide `changes()` (that is a SQLite API). Use an explicit `SELECT COUNT(*)` instead.

```python
# Wrong:
conn.execute("SELECT changes()").fetchone()[0]

# Correct:
conn.execute(
    "SELECT COUNT(*) FROM flights.flights WHERE ... AND updated_at >= ?", [now]
).fetchone()[0]
```

### TIMESTAMP values from DuckLake are timezone-naive

DuckDB returns TIMESTAMP columns as Python `datetime` without tzinfo. Comparing them with `datetime.now(timezone.utc)` (aware) via pendulum raises `TypeError: can't compare offset-naive and offset-aware datetimes`. Normalize before arithmetic:

```python
if departure.tzinfo is None:
    departure = departure.replace(tzinfo=timezone.utc)
```

### MinIO hostname in file paths is locked at table creation time

DuckLake writes absolute Parquet file paths to the PostgreSQL catalog when a table is created. If a table was created with `minio:9000`, all files are only accessible through that hostname. Changing the hostname means fully recreating the tables.

### Serving store is the right pattern for BI

DuckLake is not a serving layer by design. Reading Parquet files from S3 via the DuckLake extension on every Superset request incurs too much I/O latency. The correct approach: `docker/export-serving-store.py` atomically exports mart tables from DuckLake to `/serving/flights.duckdb`. Superset connects to this standard DuckDB file via `duckdb-engine` with no extensions required. The file is refreshed by the Airflow DAG `dag_export_serving_store` daily at 03:00 UTC after `dbt_run_daily`.

This is the same pattern used in production lakehouses: Iceberg→Redshift, Delta→Synapse.

## Comparison with Alternatives

| Aspect | DuckLake | Iceberg | Delta Lake |
|--------|----------|---------|------------|
| Metadata catalog | SQL database (PG/MySQL/SQLite) | JSON/Avro files + catalog (REST/Hive) | JSON transaction log |
| Operational complexity | Low (SQL commands) | High (manifest, snapshot, metadata files) | Medium |
| Concurrent writes | Via PG locks | Via catalog | Via log |
| Time travel | Built-in | Built-in | Built-in |
| Ecosystem | DuckDB (for now) | Spark, Trino, Flink, DuckDB | Spark, Databricks |
| Maturity | Experimental (v0.4) | Production | Production |
| Setup complexity | Very low | High | Medium |
