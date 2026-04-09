-- Плановый прилёт всегда позже планового вылета
select *
from {{ ref('stg_flights') }}
where scheduled_arrival <= scheduled_departure
