{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

-- Предагрегация бронирований по рейсу и дате.
-- Читает raw bookings напрямую чтобы избежать материализации 7.8M строк.
-- Все марты используют этот агрегат вместо int_bookings_enriched для booking_stats.
select
    b.flight_id,
    f.flight_date,
    f.src_airport_iata,
    f.dst_airport_iata,
    f.src_airport_iata || '-' || f.dst_airport_iata  as route_key,
    f.airline_iata,

    count(b.booking_id)                                                         as total_bookings,
    sum(b.price_rub)                                                             as total_revenue,
    avg(b.price_rub)                                                             as avg_ticket_price,
    count(distinct b.passenger_id)                                               as unique_passengers,

    count(case when b.fare_class = 'economy'  then 1 end)                       as economy_bookings,
    count(case when b.fare_class = 'business' then 1 end)                       as business_bookings,
    count(case when b.fare_class = 'first'    then 1 end)                       as first_bookings,

    count(case when b.booking_source = 'web'       then 1 end)                  as web_bookings,
    count(case when b.booking_source = 'mobile'    then 1 end)                  as mobile_bookings,
    count(case when b.booking_source = 'agency'    then 1 end)                  as agency_bookings,
    count(case when b.booking_source = 'corporate' then 1 end)                  as corporate_bookings,

    count(case when b.status = 'confirmed'  then 1 end)                         as confirmed_bookings,
    count(case when b.status = 'cancelled'  then 1 end)                         as cancelled_bookings,
    count(case when b.status = 'checked_in' then 1 end)                         as checked_in_bookings,
    count(case when b.status = 'boarded'    then 1 end)                         as boarded_bookings,
    count(case when b.status = 'no_show'    then 1 end)                         as no_show_bookings,

    -- дата бронирования: самая ранняя и поздняя
    min(b.booking_date::date)                                                    as first_booking_date,
    max(b.booking_date::date)                                                    as last_booking_date

from {{ ref('stg_bookings') }} b
inner join {{ ref('stg_flights') }} f on b.flight_id = f.flight_id
where b.status not in ('cancelled', 'no_show')
group by 1, 2, 3, 4, 5, 6
