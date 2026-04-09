{{
    config(
        materialized='view',
        database='flights',
        schema='main'
    )
}}

with enriched as (
    select
        ph.price_id,
        ph.flight_id,
        ph.fare_class,
        ph.price_rub,
        ph.recorded_at,
        ph.days_before_departure,

        -- бакеты дней до вылета (для анализа ценовых кривых)
        case
            when ph.days_before_departure = 0             then '0_day_of'
            when ph.days_before_departure between 1 and 6  then '1_1to6d'
            when ph.days_before_departure between 7 and 13 then '2_7to13d'
            when ph.days_before_departure between 14 and 29 then '3_14to29d'
            when ph.days_before_departure between 30 and 59 then '4_30to59d'
            else                                               '5_60plus'
        end as days_bucket,

        f.flight_date,
        f.src_airport_iata,
        f.dst_airport_iata,
        f.src_city,
        f.dst_city,
        f.airline_iata,
        f.airline_name,
        f.src_airport_iata || '-' || f.dst_airport_iata as route_key

    from {{ ref('stg_price_history') }} ph
    inner join {{ ref('int_flights_enriched') }} f on ph.flight_id = f.flight_id
)

select * from enriched
