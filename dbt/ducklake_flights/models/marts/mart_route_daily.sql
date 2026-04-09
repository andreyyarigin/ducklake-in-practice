{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

with flight_stats as (
    select
        src_airport_iata || '-' || dst_airport_iata as route_key,
        src_city || ' → ' || dst_city               as route_name,
        src_airport_iata,
        dst_airport_iata,
        flight_date,
        count(*)                                                        as total_flights,
        sum(case when status = 'cancelled' then 1 else 0 end)           as cancelled_flights,
        sum(case when status = 'delayed'   then 1 else 0 end)           as delayed_flights,
        avg(delay_minutes) filter (where delay_minutes is not null)     as avg_delay_min,
        max(delay_minutes) filter (where delay_minutes is not null)     as max_delay_min,
        sum(total_seats)                                                as total_capacity
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4, 5
),

booking_stats as (
    select
        route_key,
        flight_date,
        sum(total_bookings)     as total_bookings,
        sum(total_revenue)      as total_revenue,
        avg(avg_ticket_price)   as avg_ticket_price,
        sum(unique_passengers)  as unique_passengers,
        sum(economy_bookings)   as economy_bookings,
        sum(business_bookings)  as business_bookings,
        sum(first_bookings)     as first_bookings,
        sum(web_bookings)       as web_bookings,
        sum(mobile_bookings)    as mobile_bookings,
        sum(cancelled_bookings) as cancelled_bookings
    from {{ ref('int_bookings_daily_agg') }}
    group by 1, 2
)

select
    f.route_key,
    f.route_name,
    f.src_airport_iata,
    f.dst_airport_iata,
    f.flight_date,
    f.total_flights,
    f.cancelled_flights,
    f.delayed_flights,
    round(f.avg_delay_min, 1)                                                       as avg_delay_min,
    round(f.max_delay_min, 1)                                                       as max_delay_min,
    f.total_capacity,
    coalesce(b.total_bookings, 0)                                                   as total_bookings,
    coalesce(b.total_revenue, 0)                                                    as total_revenue,
    round(coalesce(b.avg_ticket_price, 0), 2)                                       as avg_ticket_price,
    coalesce(b.unique_passengers, 0)                                                as unique_passengers,
    coalesce(b.economy_bookings, 0)                                                 as economy_bookings,
    coalesce(b.business_bookings, 0)                                                as business_bookings,
    coalesce(b.first_bookings, 0)                                                   as first_bookings,
    coalesce(b.web_bookings, 0)                                                     as web_bookings,
    coalesce(b.mobile_bookings, 0)                                                  as mobile_bookings,
    coalesce(b.cancelled_bookings, 0)                                               as cancelled_bookings,
    case
        when f.total_capacity > 0
        then round(coalesce(b.total_bookings, 0)::float / f.total_capacity * 100, 1)
        else 0
    end                                                                             as load_factor_pct
from flight_stats f
left join booking_stats b
    on f.route_key   = b.route_key
    and f.flight_date = b.flight_date
