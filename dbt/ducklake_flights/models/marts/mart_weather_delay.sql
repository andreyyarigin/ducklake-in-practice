{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

-- mart_weather_delay: влияние погодных условий на задержки рейсов.
--
-- Этот mart существует благодаря двум новым источникам данных:
--   - weather_observations (Open-Meteo) — погода в аэропортах
--   - aircraft_types — тип и характеристики ВС
--
-- Ключевой инсайт: можно разделить задержки по причине:
--   weather-induced vs operational (маршрут/авиакомпания).

select
    flight_date,
    src_airport_iata,
    dst_airport_iata,
    src_city || ' → ' || dst_city                                       as route_name,
    airline_iata,
    airline_name,
    aircraft_family,
    aircraft_body_type,

    -- Погодный контекст вылета
    src_weather_severity,
    src_adverse_conditions,
    round(avg(src_temp_c), 1)                                           as avg_temp_c,
    round(avg(src_wind_kmh), 1)                                         as avg_wind_kmh,
    round(avg(src_visibility_km), 2)                                    as avg_visibility_km,
    round(avg(src_precipitation_mm), 2)                                 as avg_precipitation_mm,
    round(avg(src_snowfall_cm), 2)                                      as avg_snowfall_cm,

    -- Статистика задержек
    count(*)                                                            as total_flights,
    count(*) filter (where status = 'cancelled')                        as cancelled_flights,
    count(*) filter (where delay_minutes > 15)                          as delayed_flights,
    count(*) filter (where delay_minutes > 60)                          as severely_delayed_flights,

    round(
        count(*) filter (where delay_minutes > 15)::float
        / nullif(count(*) filter (where status != 'cancelled'), 0)
        * 100, 1
    )                                                                   as delay_rate_pct,

    round(
        count(*) filter (where status = 'cancelled')::float
        / nullif(count(*), 0)
        * 100, 1
    )                                                                   as cancellation_rate_pct,

    round(avg(delay_minutes) filter (where delay_minutes > 0), 1)      as avg_delay_min,
    round(max(delay_minutes) filter (where delay_minutes > 0), 1)      as max_delay_min,

    -- Задержки с разбивкой по типу погоды
    round(
        avg(delay_minutes) filter (
            where delay_minutes > 0 and src_adverse_conditions = true
        ), 1
    )                                                                   as avg_delay_adverse_weather_min,

    round(
        avg(delay_minutes) filter (
            where delay_minutes > 0 and src_adverse_conditions = false
        ), 1
    )                                                                   as avg_delay_normal_weather_min,

    -- Расчётный расход топлива (демонстрация обогащения данными ВС)
    round(
        sum(
            case
                when fuel_burn_kg_per_km is not null and scheduled_duration_minutes is not null
                then fuel_burn_kg_per_km * (scheduled_duration_minutes / 60.0 * 800.0)
                else null
            end
        ) filter (where status != 'cancelled'), 0
    )                                                                   as total_fuel_kg_estimate

from {{ ref('int_flights_enriched') }}
where src_weather_severity is not null  -- только рейсы с данными о погоде
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
