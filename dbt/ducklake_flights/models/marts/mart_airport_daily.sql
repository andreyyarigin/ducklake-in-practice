{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

with departures as (
    select
        src_airport_iata        as airport_iata,
        src_airport_name        as airport_name,
        src_city                as city,
        flight_date,
        count(*)                                                    as departures,
        sum(case when status = 'cancelled' then 1 else 0 end)      as cancelled_departures,
        avg(delay_minutes) filter (where delay_minutes is not null) as avg_departure_delay_min,
        sum(total_seats)                                           as departure_capacity
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4
),

arrivals as (
    select
        dst_airport_iata        as airport_iata,
        flight_date,
        count(*)                                                    as arrivals,
        avg(delay_minutes) filter (where delay_minutes is not null) as avg_arrival_delay_min
    from {{ ref('int_flights_enriched') }}
    group by 1, 2
)

select
    d.airport_iata,
    d.airport_name,
    d.city,
    d.flight_date,
    d.departures,
    d.cancelled_departures,
    round(d.avg_departure_delay_min, 1)         as avg_departure_delay_min,
    d.departure_capacity,
    coalesce(a.arrivals, 0)                     as arrivals,
    round(a.avg_arrival_delay_min, 1)           as avg_arrival_delay_min,
    d.departures + coalesce(a.arrivals, 0)      as total_movements
from departures d
left join arrivals a
    on d.airport_iata  = a.airport_iata
    and d.flight_date  = a.flight_date
