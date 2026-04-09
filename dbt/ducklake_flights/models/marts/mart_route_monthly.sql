{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    route_key,
    route_name,
    src_airport_iata,
    dst_airport_iata,
    date_trunc('month', flight_date)::date      as month_start,

    sum(total_flights)                          as total_flights,
    sum(cancelled_flights)                      as cancelled_flights,
    sum(delayed_flights)                        as delayed_flights,
    round(avg(avg_delay_min), 1)                as avg_delay_min,
    sum(total_capacity)                         as total_capacity,
    sum(total_bookings)                         as total_bookings,
    sum(total_revenue)                          as total_revenue,
    round(avg(avg_ticket_price), 2)             as avg_ticket_price,
    sum(unique_passengers)                      as total_passengers,
    sum(economy_bookings)                       as economy_bookings,
    sum(business_bookings)                      as business_bookings,
    sum(first_bookings)                         as first_bookings,
    case
        when sum(total_capacity) > 0
        then round(sum(total_bookings)::float / sum(total_capacity) * 100, 1)
        else 0
    end                                         as load_factor_pct,
    round(
        sum(cancelled_flights)::float / nullif(sum(total_flights), 0) * 100, 1
    )                                           as cancellation_rate_pct,
    -- топ-маршрут по выручке в месяц
    round(sum(total_revenue) / nullif(sum(total_flights), 0), 2)
                                                as revenue_per_flight

from {{ ref('mart_route_daily') }}
group by 1, 2, 3, 4, 5
