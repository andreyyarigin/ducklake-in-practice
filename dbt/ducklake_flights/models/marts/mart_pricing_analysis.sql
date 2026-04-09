{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    route_key,
    src_city || ' → ' || dst_city  as route_name,
    src_airport_iata,
    dst_airport_iata,
    airline_iata,
    airline_name,
    fare_class,
    days_bucket,
    recorded_at::date               as recorded_at,

    count(*)                        as price_observations,
    round(avg(price_rub), 2)        as avg_price_rub,
    round(min(price_rub), 2)        as min_price_rub,
    round(max(price_rub), 2)        as max_price_rub,
    round(
        percentile_cont(0.5) within group (order by price_rub), 2
    )                               as median_price_rub,
    round(stddev(price_rub), 2)     as stddev_price_rub

from {{ ref('int_price_curves') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
