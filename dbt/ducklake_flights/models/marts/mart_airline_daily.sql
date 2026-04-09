{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

with flight_agg as (
    select
        airline_iata,
        airline_name,
        flight_date,
        count(*)                                                        as total_flights,
        sum(case when status = 'cancelled' then 1 else 0 end)          as cancelled_flights,
        sum(case when status = 'delayed'   then 1 else 0 end)          as delayed_flights,
        avg(delay_minutes) filter (where delay_minutes is not null)    as avg_delay_min,
        sum(total_seats)                                               as total_capacity,
        count(distinct src_airport_iata || dst_airport_iata)           as active_routes
    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3
),

booking_agg as (
    select
        airline_iata,
        flight_date,
        sum(total_bookings)    as total_bookings,
        sum(total_revenue)     as total_revenue,
        avg(avg_ticket_price)  as avg_ticket_price,
        sum(unique_passengers) as unique_passengers
    from {{ ref('int_bookings_daily_agg') }}
    group by 1, 2
)

select
    f.airline_iata,
    f.airline_name,
    f.flight_date,
    f.total_flights,
    f.cancelled_flights,
    f.delayed_flights,
    round(f.avg_delay_min, 1)                                               as avg_delay_min,
    f.total_capacity,
    f.active_routes,
    coalesce(b.total_bookings, 0)                                           as total_bookings,
    coalesce(b.total_revenue, 0)                                            as total_revenue,
    round(coalesce(b.avg_ticket_price, 0), 2)                              as avg_ticket_price,
    coalesce(b.unique_passengers, 0)                                        as unique_passengers,
    case
        when f.total_capacity > 0
        then round(coalesce(b.total_bookings, 0)::float / f.total_capacity * 100, 1)
        else 0
    end                                                                     as load_factor_pct,
    round(
        f.cancelled_flights::float / nullif(f.total_flights, 0) * 100, 1
    )                                                                       as cancellation_rate_pct
from flight_agg f
left join booking_agg b
    on f.airline_iata  = b.airline_iata
    and f.flight_date  = b.flight_date
