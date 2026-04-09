{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    icao_code,
    iata_code,
    manufacturer,
    model,
    family,
    seats_economy,
    seats_business,
    seats_first,
    seats_total,
    range_km,
    fuel_burn_kg_per_km,
    first_flight_year,
    engine_type,
    body_type
from {{ source('raw', 'aircraft_types') }}
where icao_code is not null
