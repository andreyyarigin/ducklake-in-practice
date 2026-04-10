# dbt Layers in ducklake-in-practice

## Overview

dbt uses the `dbt-duckdb` adapter with the `is_ducklake: true` flag. All models are materialized in DuckLake (Parquet + PostgreSQL catalog). DuckDB acts as the compute engine.

```
raw → staging → intermediate → marts
```

All models use `materialized: table`. Full recomputation once per day is optimal for sandbox data volumes.

## Critical Fixes for dbt + DuckLake

### 1. DATA_PATH via a custom plugin

The standard `profiles.yml` syntax for ATTACH with DATA_PATH does not work with DuckLake. A custom plugin is used instead:

```python
# dbt/ducklake_flights/ducklake_attach_plugin.py
conn.execute("""
    ATTACH 'ducklake:postgres:host=postgres port=5432 dbname=ducklake_catalog user=ducklake password=...'
    AS flights (DATA_PATH 's3://ducklake-flights/data/')
""")
conn.execute("SET memory_limit = '6GB'")
conn.execute("SET temp_directory = '/tmp/duckdb_tmp'")
```

```yaml
# profiles.yml
ducklake-in-practice:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      plugins:
        - module: ducklake_attach_plugin
      extensions: [ducklake, postgres, httpfs]
      settings:
        s3_endpoint: "minio:9000"
        s3_access_key_id: "${MINIO_ACCESS_KEY}"
        s3_secret_access_key: "${MINIO_SECRET_KEY}"
        s3_use_ssl: false
        s3_url_style: "path"
        memory_limit: "6GB"
        temp_directory: "/tmp/duckdb_tmp"
      threads: 1
```

### 2. threads: 1 — required

DuckLake does not support concurrent writes. Setting `threads > 1` causes transaction conflicts.

### 3. drop_relation macro (CASCADE not supported)

```sql
-- macros/drop_relation.sql
{% macro drop_relation(relation) %}
  {% call statement('drop_relation') %}
    DROP {{ relation.type }} IF EXISTS {{ relation }}
  {% endcall %}
{% endmacro %}
```

### 4. staging: materialized = table

dbt opens new DuckDB connections between pipeline steps. In-memory views do not survive connection changes.

**Fix:** all staging models use `materialized: table` in `flights.main`.

### 5. OOM when running the full pipeline in one command

Running all 25 models via a single `dbt run` causes OOM with 7.8M booking rows — DuckDB keeps previous models in memory while building subsequent ones.

**Fix:** the Airflow DAG runs each layer as a separate command:
```
dbt run --select staging
dbt run --select int_bookings_daily_agg
dbt run --select intermediate
dbt run --select marts
```

The intermediate pre-aggregation model (`int_bookings_daily_agg`) reduces 7.8M → ~71K rows before mart joins.

## dbt Configuration

### Project Structure

```
dbt/ducklake_flights/
  dbt_project.yml
  profiles.yml
  ducklake_attach_plugin.py
  macros/
    drop_relation.sql
  models/
    raw/
      schema.yml              <- sources: airports, airlines, routes, flights,
                                 bookings, passengers, price_history,
                                 aircraft_types, weather_observations,
                                 route_profiles
    staging/                  <- 10 models, materialized: table
    intermediate/             <- 4 models, materialized: table (2 as view)
    marts/                    <- 10 models, materialized: table (2 as view)
  tests/
    assert_no_future_departures.sql
    assert_revenue_positive.sql
    assert_load_factor_plausible.sql
```

## Staging Layer

**Materialization:** `table` in `flights.main`

**Purpose:** cleaning, type casting, noise filtering.

| Model | What it does |
|-------|-------------|
| `stg_airports` | Filter country='Russia', trim whitespace, cast types |
| `stg_airlines` | Filter active=true, country='Russia' |
| `stg_routes` | Join with airports for validation, filter domestic |
| `stg_flights` | Cast timestamp, filter valid records |
| `stg_bookings` | Cast types, filter price_rub > 0 |
| `stg_passengers` | Cast types, normalize names |
| `stg_price_history` | Cast types, filter price_rub > 0 |
| `stg_aircraft_types` | Aircraft fleet seed, no transformations |
| `stg_weather_observations` | Filter valid records, adds `weather_severity` and `adverse_conditions` |
| `stg_route_profiles` | Route profiles seed: base_load_factor, price_tier, seasonality |

> UUID keys from the generator guarantee uniqueness — ROW_NUMBER() deduplication is not needed.

Example — `stg_bookings.sql`:

```sql
select
    booking_id, flight_id, passenger_id,
    booking_date::timestamp as booking_date, fare_class,
    cast(price_rub as decimal(10,2)) as price_rub, status,
    seat_number, booking_source,
    created_at::timestamp as created_at, updated_at::timestamp as updated_at
from {{ source('raw', 'bookings') }}
where price_rub > 0
```

## Intermediate Layer

**Purpose:** enrichment, denormalization, preparation for aggregation.

| Model | Materialization | What it does |
|-------|----------------|-------------|
| `int_flights_enriched` | `table` | flights + airports + airlines + aircraft_types + weather (18 fields) |
| `int_bookings_enriched` | `view` | bookings + flights + passengers + routes (7.8M rows — view to save memory) |
| `int_bookings_daily_agg` | `table` | Pre-aggregates bookings per flight_id → ~71K rows (7.8M → 71K) |
| `int_price_curves` | `view` | price_history with days_before_departure bucket calculations |

### Key optimization: int_bookings_daily_agg

Aggregates 7.8M bookings to the flight level before joining with flight_stats in mart models. Without this, mart_route_daily would attempt a 7.8M-row join — causing OOM.

```sql
-- int_bookings_daily_agg.sql
select
    b.flight_id, f.flight_date, f.route_key, f.airline_iata,
    count(b.booking_id)               as total_bookings,
    sum(b.price_rub)                  as total_revenue,
    avg(b.price_rub)                  as avg_ticket_price,
    count(distinct b.passenger_id)    as unique_passengers,
    count(case when b.fare_class='economy' then 1 end)  as economy_bookings,
    ...
from {{ ref('stg_bookings') }} b
inner join {{ ref('stg_flights') }} f on b.flight_id = f.flight_id
where b.status not in ('cancelled', 'no_show')
group by 1, 2, 3, 4
```

## Mart Layer

**Purpose:** business metrics, ready for serving.

### Model Table

| Model | Materialization | Grain | Metrics |
|-------|----------------|-------|---------|
| `mart_route_daily` | `table` | route × day | revenue, pax, load_factor, avg_delay, cancellation_rate |
| `mart_route_weekly` | `table` | route × week | Weekly aggregates |
| `mart_route_monthly` | `table` | route × month | Monthly aggregates |
| `mart_airline_daily` | `table` | airline × day | revenue, flights, cancellation_rate, avg_delay |
| `mart_airport_daily` | `table` | airport × day | departures, arrivals, avg_delay, on_time_rate |
| `mart_booking_funnel` | `table` | day × source × class | confirmed → checked_in → boarded conversion |
| `mart_pricing_analysis` | `table` | route × days_bucket | Average prices by days-before-departure bucket |
| `mart_delay_analysis` | `table` | airline × airport × month | Delay statistics by carrier and airport |
| `mart_passenger_segments` | `view` | passenger | RFM segmentation (6.5M rows — view) |
| `mart_passenger_segment_stats` | `table` | segment | Segment aggregates (4 rows) — for BI dashboard |
| `mart_weather_delay` | `table` | route × airline × weather | Delays vs weather conditions; aircraft type breakdown |

> `mart_passenger_segments` — `view` due to 6.5M rows. Excluded from serving store export.
> `mart_passenger_segment_stats` — aggregated on top of it: 4 rows (one per segment). Included in serving store.

### Example: mart_route_daily

```sql
with flight_stats as (
    select
        src_airport_iata || '-' || dst_airport_iata as route_key,
        src_city || ' → ' || dst_city               as route_name,
        src_airport_iata, dst_airport_iata, flight_date,
        count(*)                                     as total_flights,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_flights,
        avg(delay_minutes) filter (where delay_minutes is not null) as avg_delay_min,
        sum(total_seats)                             as total_capacity
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4, 5   -- airline_iata NOT in GROUP BY: otherwise LF > 100%
),
booking_stats as (
    select route_key, flight_date, sum(total_bookings), sum(total_revenue), ...
    from {{ ref('int_bookings_daily_agg') }}
    group by 1, 2
)
select
    f.route_key, f.route_name, f.flight_date,
    ...,
    case when f.total_capacity > 0
         then round(coalesce(b.total_bookings, 0)::float / f.total_capacity * 100, 1)
         else 0 end as load_factor_pct
from flight_stats f
left join booking_stats b on f.route_key = b.route_key and f.flight_date = b.flight_date
```

> **Important:** `airline_iata` is excluded from `flight_stats` GROUP BY. Including it would split each route/date into per-airline rows, while `booking_stats` is aggregated at route_key+date level. The join would assign the full booking total to each airline row → LF > 100%.

## dbt Tests (83/83 PASS)

### Schema tests (schema.yml)

| Layer | Tests | Examples |
|-------|-------|---------|
| staging | 39 | unique/not_null on PKs, relationships, accepted_values |
| intermediate | 11 | unique/not_null on PKs |
| marts | 33 | accepted_range for LF, revenue, percentage metrics |

### Data tests (SQL)

```sql
-- tests/assert_no_future_departures.sql
select * from {{ ref('stg_flights') }}
where actual_departure > current_timestamp + interval '1 hour'

-- tests/assert_revenue_positive.sql
select * from {{ ref('mart_route_daily') }}
where total_revenue < 0

-- tests/assert_load_factor_plausible.sql
-- up to 10% overbooking is normal in aviation
select * from {{ ref('mart_route_daily') }}
where load_factor_pct > 110
```

## Airflow Run Schedule

| DAG | Schedule | Steps |
|-----|----------|-------|
| `dbt_run_daily` | Daily at 02:00 UTC | **deps** → staging → int_bookings_daily_agg → intermediate → marts |
| `dbt_test_daily` | Daily at 03:30 UTC | **deps** → test staging → test intermediate → test marts |
| `dbt_docs_weekly` | Sundays at 04:00 UTC | dbt docs generate |

Layers run as **separate commands** sequentially — not a single `dbt run --select all`. This prevents OOM when processing 7.8M booking rows.

`dbt deps` runs as the first task in each DAG to ensure packages (dbt_utils) are installed after container restarts.
