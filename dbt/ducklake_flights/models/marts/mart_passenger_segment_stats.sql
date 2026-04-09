{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

-- Агрегат по сегментам пассажиров для BI-дашборда.
-- Лёгкий вариант mart_passenger_segments (4 строки вместо 6.5M) — пригоден для serving store.
select
    segment,
    count(passenger_id)                        as passenger_count,
    round(avg(total_bookings), 1)              as avg_bookings,
    round(avg(avg_ticket_price_rub), 0)        as avg_price_rub,
    round(sum(total_revenue_rub), 0)           as total_revenue,
    round(avg(cancellation_rate_pct), 1)       as avg_cancellation_rate_pct,
    round(avg(economy_bookings::float
              / nullif(total_bookings, 0) * 100), 1) as economy_share_pct,
    round(avg(business_bookings::float
              / nullif(total_bookings, 0) * 100), 1) as business_share_pct,
    round(avg(web_bookings::float
              / nullif(total_bookings, 0) * 100), 1) as web_share_pct,
    round(avg(mobile_bookings::float
              / nullif(total_bookings, 0) * 100), 1) as mobile_share_pct
from {{ ref('mart_passenger_segments') }}
group by segment
