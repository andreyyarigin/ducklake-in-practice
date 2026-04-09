# ducklake-in-practice Data Model

## Overview

Data is split into two categories:

- **Reference data (seed)** — loaded from OpenFlights once, updated infrequently
- **Transactional data (generated)** — produced by a Python generator on a schedule (~500 MB Parquet/day)

Scope: Russian domestic flights. Scaling path: Moscow → Russia → CIS via seed expansion.

## Entity Relationships

```
airports ──┬──────────────────────────────────────────────────────┐
           │                                                      │
airlines ──┼── routes                  aircraft_types            │
           │     │                         │                     │
           │     ▼                         │                     │
           └── flights ──┬── bookings ──── passengers            │
                    │    │                                        │
                    │    └── price_history                        │
                    │                                             │
airports ───────────────────────────────────────────────────────--┘
(src + dst)         │
                    ▼
           weather_observations
           (airport × date)
```

## Seed Data (OpenFlights)

### airports

Source: `https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat`

| Column | Type | Description |
|--------|------|-------------|
| airport_id | INTEGER | PK, OpenFlights ID |
| name | VARCHAR | Airport name |
| city | VARCHAR | City |
| country | VARCHAR | Country |
| iata_code | VARCHAR(3) | IATA code (SVO, DME, LED...) |
| icao_code | VARCHAR(4) | ICAO code |
| latitude | DOUBLE | Latitude |
| longitude | DOUBLE | Longitude |
| altitude | INTEGER | Altitude (feet) |
| timezone_offset | DOUBLE | UTC offset |
| timezone_name | VARCHAR | Timezone (Europe/Moscow) |

Load filter: `country = 'Russia'`.

Key airports: SVO (Sheremetyevo), DME (Domodedovo), VKO (Vnukovo), ZIA (Zhukovsky), LED (Pulkovo), KZN (Kazan), SVX (Koltsovo), OVB (Tolmachevo), KRR (Krasnodar), AER (Sochi), ROV (Platov), UFA (Ufa), GOJ (Strigino), KUF (Kurumoch), CEK (Chelyabinsk).

### airlines

Source: `https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat`

| Column | Type | Description |
|--------|------|-------------|
| airline_id | INTEGER | PK |
| name | VARCHAR | Airline name |
| iata_code | VARCHAR(2) | IATA code (SU, S7, DP, UT, U6...) |
| icao_code | VARCHAR(3) | ICAO code |
| country | VARCHAR | Country |
| active | BOOLEAN | Currently operating |

Filter: `country = 'Russia' AND active = true`.

### routes

Source: `https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat`

| Column | Type | Description |
|--------|------|-------------|
| airline_iata | VARCHAR(2) | FK → airlines |
| src_airport_iata | VARCHAR(3) | FK → airports |
| dst_airport_iata | VARCHAR(3) | FK → airports |
| codeshare | BOOLEAN | Codeshare flight |
| stops | INTEGER | Stops (0 = direct) |
| equipment | VARCHAR | Aircraft types (320, 73H...) |

Filter: both airports are Russian.

## Transactional Data (Generated)

### flights

Generated daily. Schedule covers 7 days ahead.

| Column | Type | Description |
|--------|------|-------------|
| flight_id | VARCHAR | PK, UUID |
| flight_number | VARCHAR | SU-1234 |
| airline_iata | VARCHAR(2) | FK → airlines |
| src_airport_iata | VARCHAR(3) | FK → airports |
| dst_airport_iata | VARCHAR(3) | FK → airports |
| scheduled_departure | TIMESTAMP | Scheduled departure |
| scheduled_arrival | TIMESTAMP | Scheduled arrival |
| actual_departure | TIMESTAMP | Actual departure (NULL before departure) |
| actual_arrival | TIMESTAMP | Actual arrival |
| status | VARCHAR | scheduled/boarding/departed/arrived/cancelled/delayed |
| aircraft_type | VARCHAR | A320, B738, SU95... |
| total_seats | INTEGER | Total seat count |
| flight_date | DATE | Flight date (partition key) |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Record update time |

Partitioned by `flight_date`.

### bookings

Generated 4 times per day (00:15, 06:15, 12:15, 18:15 UTC). Each batch covers flights from yesterday through +90 days.

| Column | Type | Description |
|--------|------|-------------|
| booking_id | VARCHAR | PK, UUID |
| flight_id | VARCHAR | FK → flights |
| passenger_id | VARCHAR | FK → passengers |
| booking_date | TIMESTAMP | Booking date |
| fare_class | VARCHAR | economy/business/first |
| price_rub | DECIMAL(10,2) | Price in rubles |
| status | VARCHAR | confirmed/cancelled/checked_in/boarded/no_show |
| seat_number | VARCHAR | 12A, 28F... (NULL before check-in) |
| booking_source | VARCHAR | web/mobile/agency/corporate |
| created_at | TIMESTAMP | Creation time |
| updated_at | TIMESTAMP | Update time |

### passengers

| Column | Type | Description |
|--------|------|-------------|
| passenger_id | VARCHAR | PK, UUID |
| first_name | VARCHAR | First name |
| last_name | VARCHAR | Last name |
| email | VARCHAR | Email (may contain errors) |
| phone | VARCHAR | Phone number |
| date_of_birth | DATE | Date of birth |
| frequent_flyer_id | VARCHAR | Loyalty program number (NULL for ~70%) |
| created_at | TIMESTAMP | Creation time |

### price_history

Generated alongside bookings. Tracks price changes per route.

| Column | Type | Description |
|--------|------|-------------|
| price_id | VARCHAR | PK, UUID |
| flight_id | VARCHAR | FK → flights |
| fare_class | VARCHAR | economy/business/first |
| price_rub | DECIMAL(10,2) | Price |
| recorded_at | TIMESTAMP | Recording timestamp |
| days_before_departure | INTEGER | Days before departure |

## Seed Data: Aircraft Types (aircraft_types)

Source: `data/seeds/aircraft_types.csv` — public technical specifications.

| Column | Type | Description |
|--------|------|-------------|
| icao_code | VARCHAR(4) | PK (A320, B738, SU95...) |
| iata_code | VARCHAR(3) | IATA code |
| manufacturer | VARCHAR | Manufacturer (Airbus, Boeing, Sukhoi...) |
| model | VARCHAR | Model (A320-200, 737-800...) |
| family | VARCHAR | Family (A320, B737NG, SSJ100...) |
| seats_economy / business / first | INTEGER | Seats by class |
| seats_total | INTEGER | Total seats |
| range_km | INTEGER | Maximum range, km |
| fuel_burn_kg_per_km | DOUBLE | Fuel burn, kg/km |
| first_flight_year | INTEGER | Year of type's first flight |
| engine_type | VARCHAR | turbofan / turboprop |
| body_type | VARCHAR | narrowbody / widebody |

17 aircraft types: A319/320/321, A320neo, A321neo, A330-200/300, B737-700/800, B767, B777, SSJ100, E170/190, ATR72, CRJ200.

## Transactional Data: Weather Observations (weather_observations)

Source: Open-Meteo Archive API (free, no API key). Loaded daily by DAG `ingest_weather`.

| Column | Type | Description |
|--------|------|-------------|
| observation_id | VARCHAR | PK, UUID |
| airport_iata | VARCHAR(3) | FK → airports |
| observation_date | DATE | Date (partition key) |
| temperature_min/max/mean_c | DOUBLE | Temperature, °C |
| precipitation_mm | DOUBLE | Precipitation, mm |
| windspeed_max_kmh | DOUBLE | Max wind speed, km/h |
| windgusts_max_kmh | DOUBLE | Wind gusts, km/h |
| visibility_min_km | DOUBLE | Min visibility, km |
| snowfall_cm | DOUBLE | Snowfall, cm |
| weather_code | INTEGER | WMO weather code |
| weather_description | VARCHAR | Description in Russian |
| fetched_at | TIMESTAMP | Fetch timestamp |

**Partitioned** by `observation_date`.

## Partitioning Strategy

| Table | Partition Key | Logic |
|-------|--------------|-------|
| flights | flight_date | By flight date |
| bookings | flight_date | By flight date (via FK to flights) |
| price_history | flight_date | By flight date |
| weather_observations | observation_date | By observation date |
| passengers | — | No partitioning (reference table) |
| airports | — | Seed data, not partitioned |
| airlines | — | Seed data, not partitioned |
| routes | — | Seed data, not partitioned |
| aircraft_types | — | Seed data, not partitioned |

File structure in MinIO:
```
s3://ducklake-flights/data/main/
  flights/flight_date=2025-01-01/data_xxxxx.parquet
  bookings/flight_date=2025-01-01/...
  price_history/flight_date=2025-01-01/...
  weather_observations/observation_date=2025-01-01/...
```

## Generator Business Logic

### Seasonality
- Summer (June–August): ×1.4 base price, ×1.3 booking volume
- New Year (Dec 25 – Jan 10): ×1.6 price, ×1.5 bookings
- Southern destinations (AER, KRR) in winter: additional ×1.3

### Dynamic Pricing
- 60+ days before departure: ×0.7 of base price
- 30–60 days: ×0.85
- 14–30 days: ×1.0
- 7–14 days: ×1.2
- 1–7 days: ×1.5
- Departure day: ×2.0

### Delays and Cancellations
- ~15% of flights are delayed (normal distribution, μ=30 min, σ=45 min)
- ~2.5% of flights are cancelled
- Winter delays ×1.5 (weather)

## Intentional Data Quality Issues

The data contains deliberate defects to demonstrate dbt tests and staging-layer handling.

| Defect | Frequency | Table | Handling in staging |
|--------|-----------|-------|---------------------|
| Duplicate bookings | ~1% | bookings | UUID keys from the generator guarantee uniqueness; filter WHERE price_rub > 0 |
| Empty email | ~3% | passengers | Filter or replace with NULL |
| Negative price | ~0.5% | bookings, price_history | WHERE price_rub > 0 |
| Delayed status update | ~2% of flights | flights | Stays as scheduled after actual departure |
| Missing actual_departure | ~2% | flights | actual_departure may appear 1–2 batches late |

## Data Volumes

| Table | Records/day | Parquet size/day |
|-------|-------------|-----------------|
| flights | ~800 | ~2 MB |
| bookings | ~4,000 | ~15 MB |
| passengers | ~3,500 | ~10 MB |
| price_history | ~12,000 | ~30 MB |
| weather_observations | ~50 (airports) | ~0.1 MB |

Total (~500 MB/day is achieved via more frequent price_history writes and auxiliary events).

Current data volume: ~99,200 flights, ~7.8M bookings, ~213,600 price_history records, ~17,000 weather observations.

## TTL and Data Lifecycle

| Layer | TTL | Mechanism |
|-------|-----|-----------|
| raw | 7 days | `ducklake_expire_snapshots` + `ducklake_cleanup_old_files` |
| staging | — | Overwritten on each dbt run |
| intermediate | Indefinite | Incremental accumulation |
| marts | Indefinite | Incremental accumulation |
