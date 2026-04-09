{{
    config(
        materialized='view',
        database='flights',
        schema='main'
    )
}}

-- Сегментация пассажиров по активности бронирований.
-- Агрегация только по stg_bookings (без JOIN на flights) для эффективности.
-- favorite_route и unique_routes берутся из int_bookings_daily_agg.
with booking_agg as (
    select
        passenger_id,
        count(*)                                                         as total_bookings,
        count(*) filter (where status = 'confirmed')                     as confirmed_bookings,
        count(*) filter (where status = 'cancelled')                     as cancelled_bookings,
        round(
            count(*) filter (where status = 'cancelled')::float
            / nullif(count(*), 0) * 100, 1
        )                                                                as cancellation_rate_pct,
        round(sum(price_rub), 2)                                         as total_revenue_rub,
        round(avg(price_rub), 2)                                         as avg_ticket_price_rub,
        count(*) filter (where fare_class = 'economy')                   as economy_bookings,
        count(*) filter (where fare_class = 'business')                  as business_bookings,
        count(*) filter (where fare_class = 'first')                     as first_bookings,
        count(*) filter (where booking_source = 'web')                   as web_bookings,
        count(*) filter (where booking_source = 'mobile')                as mobile_bookings,
        min(booking_date::date)                                          as first_booking_date,
        max(booking_date::date)                                          as last_booking_date
    from {{ ref('stg_bookings') }}
    group by passenger_id
),

route_agg as (
    select
        b.passenger_id,
        count(distinct a.route_key)                                      as unique_routes,
        count(distinct a.airline_iata)                                   as unique_airlines,
        mode() within group (order by a.route_key)                       as favorite_route,
        mode() within group (order by a.airline_iata)                    as favorite_airline
    from {{ ref('stg_bookings') }} b
    inner join {{ ref('int_bookings_daily_agg') }} a on b.flight_id = a.flight_id
    group by b.passenger_id
)

select
    ba.passenger_id,
    p.first_name || ' ' || p.last_name   as passenger_name,
    p.frequent_flyer_id                   as loyalty_id,
    ba.total_bookings,
    ba.confirmed_bookings,
    ba.cancelled_bookings,
    ba.cancellation_rate_pct,
    ba.total_revenue_rub,
    ba.avg_ticket_price_rub,
    coalesce(ra.unique_routes, 0)         as unique_routes,
    coalesce(ra.unique_airlines, 0)       as unique_airlines,
    ba.economy_bookings,
    ba.business_bookings,
    ba.first_bookings,
    ba.web_bookings,
    ba.mobile_bookings,
    ba.first_booking_date,
    ba.last_booking_date,
    ra.favorite_route,
    ra.favorite_airline,
    case
        when ba.total_bookings >= 10 then 'frequent'
        when ba.total_bookings >= 4  then 'regular'
        when ba.total_bookings >= 2  then 'occasional'
        else 'one_time'
    end                                   as segment
from booking_agg ba
left join route_agg ra on ba.passenger_id = ra.passenger_id
left join {{ ref('stg_passengers') }} p  on ba.passenger_id = p.passenger_id
