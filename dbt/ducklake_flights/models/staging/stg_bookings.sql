-- stg_bookings: фильтрация и приведение типов.
-- Дедупликация по booking_id (UUID) — генератор намеренно создаёт ~1% дублей
-- с разными booking_id, поэтому дедупликация по booking_id достаточна.
select
    booking_id,
    flight_id,
    passenger_id,
    booking_date::timestamp     as booking_date,
    fare_class,
    cast(price_rub as decimal(10, 2)) as price_rub,
    status,
    seat_number,
    booking_source,
    created_at::timestamp       as created_at,
    updated_at::timestamp       as updated_at
from {{ source('raw', 'bookings') }}
where price_rub > 0  -- фильтр намеренных отрицательных цен
