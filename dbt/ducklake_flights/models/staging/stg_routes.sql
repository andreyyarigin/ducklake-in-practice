with source as (
    select * from {{ source('raw', 'routes') }}
),

airports as (
    select iata_code from {{ ref('stg_airports') }}
),

-- только маршруты между российскими аэропортами
domestic as (
    select
        r.airline_iata,
        r.src_airport_iata,
        r.dst_airport_iata,
        r.codeshare,
        r.stops,
        r.equipment
    from source r
    inner join airports src_ap on r.src_airport_iata = src_ap.iata_code
    inner join airports dst_ap on r.dst_airport_iata = dst_ap.iata_code
    where r.src_airport_iata <> r.dst_airport_iata
)

select * from domestic
