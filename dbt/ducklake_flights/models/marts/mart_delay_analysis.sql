{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    src_airport_iata || '-' || dst_airport_iata                         as route_key,
    src_city || ' → ' || dst_city                                       as route_name,
    src_airport_iata,
    dst_airport_iata,
    airline_iata,
    airline_name,
    flight_date,
    isodow(flight_date)                                                  as day_of_week,
    extract(hour from scheduled_departure)::int                          as hour_of_day,

    count(*)                                                             as total_flights,
    count(*) filter (where delay_minutes > 15)                           as delayed_flights,
    count(*) filter (where status = 'cancelled')                         as cancelled_flights,

    round(
        avg(delay_minutes) filter (where delay_minutes > 0), 1
    )                                                                    as avg_delay_min,
    round(
        approx_quantile(delay_minutes, 0.75) filter (where delay_minutes > 0), 1
    )                                                                    as p75_delay_min,
    round(
        approx_quantile(delay_minutes, 0.95) filter (where delay_minutes > 0), 1
    )                                                                    as p95_delay_min,
    round(
        max(delay_minutes) filter (where delay_minutes > 0), 1
    )                                                                    as max_delay_min,

    round(
        count(*) filter (where status != 'cancelled' and (delay_minutes is null or delay_minutes <= 15))
            ::float
        / nullif(count(*) filter (where status != 'cancelled'), 0)
        * 100,
        1
    )                                                                    as on_time_pct,

    round(
        count(*) filter (where status = 'cancelled')::float
        / nullif(count(*), 0)
        * 100,
        1
    )                                                                    as cancellation_rate_pct

from {{ ref('int_flights_enriched') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
