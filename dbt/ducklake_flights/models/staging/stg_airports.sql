with source as (
    select * from {{ source('raw', 'airports') }}
),

cleaned as (
    select
        airport_id,
        trim(name)          as name,
        trim(city)          as city,
        country,
        upper(trim(iata_code))  as iata_code,
        upper(trim(icao_code))  as icao_code,
        latitude,
        longitude,
        altitude,
        timezone_offset,
        timezone_name
    from source
    where country = 'Russia'
      and iata_code is not null
      and length(trim(iata_code)) = 3
)

select * from cleaned
