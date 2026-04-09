{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    f.flight_id,
    f.flight_number,
    f.flight_date,
    f.airline_iata,
    f.src_airport_iata,
    f.dst_airport_iata,
    f.status,
    f.aircraft_type,
    f.total_seats,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.actual_departure,
    f.actual_arrival,

    a_src.name          as src_airport_name,
    a_src.city          as src_city,
    a_src.latitude      as src_latitude,
    a_src.longitude     as src_longitude,

    a_dst.name          as dst_airport_name,
    a_dst.city          as dst_city,
    a_dst.latitude      as dst_latitude,
    a_dst.longitude     as dst_longitude,

    al.name             as airline_name,

    -- задержка в минутах (NULL если нет фактического вылета)
    case
        when f.actual_departure is not null
        then extract(epoch from (f.actual_departure - f.scheduled_departure)) / 60.0
        else null
    end                 as delay_minutes,

    -- продолжительность полёта в минутах
    extract(epoch from (f.scheduled_arrival - f.scheduled_departure)) / 60.0
                        as scheduled_duration_minutes,

    -- характеристики воздушного судна
    ac.manufacturer     as aircraft_manufacturer,
    ac.model            as aircraft_model,
    ac.family           as aircraft_family,
    ac.seats_total      as aircraft_seats_total,
    ac.range_km         as aircraft_range_km,
    ac.fuel_burn_kg_per_km,
    ac.body_type        as aircraft_body_type,
    ac.engine_type      as aircraft_engine_type,

    -- погода в аэропорту вылета
    w_src.temperature_mean_c    as src_temp_c,
    w_src.windspeed_max_kmh     as src_wind_kmh,
    w_src.visibility_min_km     as src_visibility_km,
    w_src.precipitation_mm      as src_precipitation_mm,
    w_src.snowfall_cm           as src_snowfall_cm,
    w_src.weather_severity      as src_weather_severity,
    w_src.adverse_conditions    as src_adverse_conditions,

    -- погода в аэропорту назначения
    w_dst.temperature_mean_c    as dst_temp_c,
    w_dst.windspeed_max_kmh     as dst_wind_kmh,
    w_dst.visibility_min_km     as dst_visibility_km,
    w_dst.adverse_conditions    as dst_adverse_conditions,

    f.created_at,
    f.updated_at

from {{ ref('stg_flights') }} f
left join {{ ref('stg_airports') }}           a_src on f.src_airport_iata = a_src.iata_code
left join {{ ref('stg_airports') }}           a_dst on f.dst_airport_iata = a_dst.iata_code
left join {{ ref('stg_airlines') }}           al    on f.airline_iata      = al.iata_code
left join {{ ref('stg_aircraft_types') }}     ac    on f.aircraft_type     = ac.icao_code
left join {{ ref('stg_weather_observations') }} w_src
    on f.src_airport_iata = w_src.airport_iata
   and f.flight_date      = w_src.observation_date
left join {{ ref('stg_weather_observations') }} w_dst
    on f.dst_airport_iata = w_dst.airport_iata
   and f.flight_date      = w_dst.observation_date
