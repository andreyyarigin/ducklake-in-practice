-- Фактический вылет не должен быть в будущем (более 1 часа от now)
select *
from {{ ref('stg_flights') }}
where actual_departure > current_timestamp + interval '1 hour'
