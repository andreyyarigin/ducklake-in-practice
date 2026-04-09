{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

with source as (
    select * from {{ source('raw', 'weather_observations') }}
),

deduplicated as (
    -- Если погода за одну дату/аэропорт загружалась повторно — берём последнюю запись
    select *,
        row_number() over (
            partition by airport_iata, observation_date
            order by fetched_at desc
        ) as rn
    from source
    where observation_id is not null
      and observation_date is not null
      and airport_iata is not null
)

select
    observation_id,
    airport_iata,
    observation_date::date              as observation_date,
    temperature_min_c,
    temperature_max_c,
    temperature_mean_c,
    precipitation_mm,
    windspeed_max_kmh,
    windgusts_max_kmh,
    visibility_min_km,
    snowfall_cm,
    weather_code,
    weather_description,
    fetched_at::timestamp               as fetched_at,

    -- Категории погодных условий для аналитики
    case
        when visibility_min_km < 1.0                    then 'low_visibility'
        when windspeed_max_kmh > 60                     then 'strong_wind'
        when precipitation_mm > 20                      then 'heavy_precipitation'
        when snowfall_cm > 5                            then 'heavy_snow'
        when temperature_mean_c < -20                   then 'extreme_cold'
        when weather_code in (95, 96, 99)               then 'thunderstorm'
        when weather_code in (45, 48)                   then 'fog'
        else 'normal'
    end                                 as weather_severity,

    -- Флаг: условия, типично влияющие на задержки
    case
        when visibility_min_km < 1.0
          or windspeed_max_kmh > 60
          or snowfall_cm > 5
          or weather_code in (45, 48, 95, 96, 99)
        then true
        else false
    end                                 as adverse_conditions

from deduplicated
where rn = 1
