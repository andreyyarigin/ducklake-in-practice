{{
    config(
        materialized='view',
        database='flights',
        schema='main'
    )
}}

select
    b.booking_id,
    b.flight_id,
    b.passenger_id,
    b.booking_date,
    b.fare_class,
    b.price_rub,
    b.status,
    b.seat_number,
    b.booking_source,

    -- из рейса
    f.flight_date,
    f.flight_number,
    f.airline_iata,
    f.airline_name,
    f.src_airport_iata,
    f.dst_airport_iata,
    f.src_city,
    f.dst_city,
    f.src_airport_name,
    f.dst_airport_name,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.status                as flight_status,
    f.aircraft_type,
    f.total_seats,

    -- из пассажира
    p.first_name,
    p.last_name,
    p.frequent_flyer_id,

    -- маршрутный ключ для агрегатов
    f.src_airport_iata || '-' || f.dst_airport_iata as route_key,
    f.src_city || ' → ' || f.dst_city               as route_name,

    -- дней до вылета на момент бронирования
    datediff('day', b.booking_date::date, f.flight_date) as days_before_departure,

    b.created_at

from {{ ref('stg_bookings') }} b
inner join {{ ref('int_flights_enriched') }} f on b.flight_id = f.flight_id
left  join {{ ref('stg_passengers') }} p        on b.passenger_id = p.passenger_id
