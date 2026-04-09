-- stg_flights: фильтрация и приведение типов.
-- UUID flight_id гарантирует уникальность из генератора.
select
    flight_id,
    flight_number,
    airline_iata,
    src_airport_iata,
    dst_airport_iata,
    scheduled_departure::timestamp  as scheduled_departure,
    scheduled_arrival::timestamp    as scheduled_arrival,
    actual_departure::timestamp     as actual_departure,
    actual_arrival::timestamp       as actual_arrival,
    status,
    aircraft_type,
    total_seats,
    flight_date::date               as flight_date,
    created_at::timestamp           as created_at,
    updated_at::timestamp           as updated_at
from {{ source('raw', 'flights') }}
where flight_id is not null
  and flight_date is not null
  and scheduled_departure < scheduled_arrival
