-- Суммарная выручка по маршруту за день не должна быть отрицательной
select *
from {{ ref('mart_route_daily') }}
where total_revenue < 0
