{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

-- Воронка бронирований по дате, источнику и классу.
-- Читает stg_bookings напрямую (не через int_bookings_enriched) для эффективности.
select
    booking_date::date       as booking_date,
    booking_source,
    fare_class,

    count(*)                                                        as total_bookings,

    -- воронка по статусам
    count(case when status = 'confirmed'  then 1 end)              as confirmed,
    count(case when status = 'checked_in' then 1 end)              as checked_in,
    count(case when status = 'boarded'    then 1 end)              as boarded,
    count(case when status = 'cancelled'  then 1 end)              as cancelled,
    count(case when status = 'no_show'    then 1 end)              as no_show,

    -- конверсии
    round(
        count(case when status in ('checked_in', 'boarded') then 1 end)::float
        / nullif(count(*), 0) * 100, 1
    )                                                               as checkin_conversion_pct,

    round(
        count(case when status = 'boarded' then 1 end)::float
        / nullif(count(case when status in ('checked_in', 'boarded') then 1 end), 0) * 100, 1
    )                                                               as boarding_conversion_pct,

    -- выручка
    sum(price_rub)                                                  as total_revenue,
    round(avg(price_rub), 2)                                        as avg_price_rub

from {{ ref('stg_bookings') }}
group by 1, 2, 3
