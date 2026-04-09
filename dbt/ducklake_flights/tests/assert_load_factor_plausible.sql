-- Load factor не должен превышать 110% (overbooking до 10% — норма для авиации)
select *
from {{ ref('mart_route_daily') }}
where load_factor_pct > 110
