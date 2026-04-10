# Scaling ducklake-in-practice

## Current Limitations

### DuckDB: single-node, single-process

DuckDB runs on a single machine. No cluster, no sharding. Each client (FastAPI, Superset, dbt) runs its own DuckDB process. They do not share compute resources.

**Concurrency:** DuckDB is optimized for large, infrequent queries. Running many small concurrent queries quickly is not its strength. The model is: one writer, many readers within a single process.

**Project workaround:** dbt pre-computes aggregates → serving reads small mart tables → DuckDB handles lightweight serving queries well.

### DuckLake: experimental (v0.4)

DuckLake is not production-ready (expected stabilization: 2026). Key issues:
- Join performance on Parquet is worse than native DuckDB (cardinality estimation errors)
- No constraints, keys, or indexes
- `threads: 1` is required — no concurrent write support
- Does not support CASCADE DROP
- Does not support `executemany()` (temp table + INSERT SELECT required)

### Data Volume

Current: ~3.5 GB/week (500 MB/day × 7-day raw TTL). DuckDB is comfortable with up to ~100–200 GB on a single machine (depending on RAM). Beyond that, a different strategy is needed.

### Write Throughput

Two Airflow workers, `threads: 1` in dbt. Ingestion and transformation are sequential. At the current volume (500 MB/day) this is not a bottleneck. At 10x growth, delays will appear.

## Scaling Paths

### Path 1: Expanding Data (Moscow → Russia → CIS)

**What changes:** more airports, routes, flights. Data volume grows linearly.

**What to do:**
- Add partitioning by `src_airport_iata` (second level after `flight_date`)
- Scale Airflow workers to 3–4
- Increase RAM for DuckDB containers

**DuckLake handles this:** linear growth does not require architectural changes.

### Path 2: More Analysts (>10 users)

**Problem:** DuckDB in-process does not scale to dozens of concurrent Superset/FastAPI users.

**Option A: MotherDuck (managed DuckDB)**

```yaml
# profiles.yml
dev:
  type: duckdb
  path: "md:ducklake_flights?motherduck_token=${MOTHERDUCK_TOKEN}"
```

MotherDuck is a server-based DuckDB with concurrent access. Data stays in DuckLake. Minimal code changes. Recommended as the first scaling step.

**Option B: ClickHouse serving**

```sql
-- ClickHouse reads Parquet from MinIO directly
CREATE TABLE mart_route_daily
ENGINE = S3('http://minio:9000/ducklake-flights/data/main/mart_route_daily/*.parquet', 'Parquet')
```

ClickHouse as the serving layer. DuckLake remains the source of truth. Superset connects to ClickHouse. Scales horizontally.

**Option C: PostgreSQL materialized views**

```sql
-- For simple dashboards
CREATE MATERIALIZED VIEW mart_route_daily AS
SELECT * FROM ducklake_export('...');

REFRESH MATERIALIZED VIEW CONCURRENTLY mart_route_daily;
```

Good for teams already using PostgreSQL. Native Superset support, no new systems required.

### Path 3: Sub-Second Latency for the API

**Problem:** DuckDB is not an OLTP system. For APIs with strict SLAs (~10–50ms), a cache is needed.

**Solution:** Redis cache on top of mart tables.

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

Cache TTL is 5 minutes (mart updates are daily, cache stays fresh between updates).

### Path 4: Real-Time (Replacing Batch with Streaming)

**Problem:** data is available with a 1-hour delay.

**Solution:** streaming is outside the scope of DuckLake. Lambda/kappa architecture:

```
Kafka
  ├── Flink/Spark Streaming → ClickHouse (real-time, <1 min latency)
  └── Batch → DuckLake (historical analytics, time travel)
```

DuckLake remains for long-term storage and analytics. Real-time serving is a separate stack.

### Path 5: >1 TB of Data

**Problem:** single-machine DuckDB is constrained by RAM.

**Solution:** migrate to Spark + Iceberg or MotherDuck with separated compute/storage.

```
DuckLake (Parquet + PG catalog)
    ↓ (migrate Parquet files)
Iceberg (same Parquet, different catalog)
    ↓
Spark (distributed compute)
```

Migration does not require reformatting files — Parquet stays Parquet. The switch involves adopting an Iceberg catalog and setting up a Spark cluster.

## Decision Matrix

| Scenario | Recommendation | Complexity |
|----------|---------------|-----------|
| 1–5 analysts, <100 GB | Current architecture | Ready now |
| 5–20 analysts, <500 GB | MotherDuck | Low |
| 20+ analysts, dashboards | ClickHouse serving | Medium |
| Sub-second API | Redis cache + marts | Medium |
| Real-time + analytics | Kafka + ClickHouse + DuckLake | High |
| >1 TB of data | MotherDuck or Spark + Iceberg | High |

## Honest Assessment

### Where DuckDB/DuckLake excels

- Ad-hoc analytics for a single analyst — faster than almost anything else
- dbt transformations — simpler and cheaper than Spark
- Local development — `pip install duckdb` and everything works
- Operational simplicity — no cluster, no ZooKeeper, no coordinator
- Time travel and schema evolution — more elegant than Iceberg

### Where DuckDB/DuckLake hits a wall

- High-concurrency serving (>10 concurrent queries)
- Data exceeding ~200 GB on a single machine
- Sub-second API latency without a cache
- Real-time ingestion
- Multi-writer from separate processes (DuckLake improves this via PostgreSQL, but does not fully solve it)
- Production-grade reliability (v0.4 is experimental)

### Conclusion

DuckDB/DuckLake is an excellent choice for sandboxes, proof-of-concepts, and small analytics platforms (up to 5 analysts, up to 100 GB). As requirements grow, the stack evolves incrementally: MotherDuck → ClickHouse serving → Spark + Iceberg. The transition is gradual: Parquet files do not need to be rewritten.
