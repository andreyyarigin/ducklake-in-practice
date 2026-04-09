with source as (
    select * from {{ source('raw', 'airlines') }}
),

cleaned as (
    select
        airline_id,
        trim(name)              as name,
        upper(trim(iata_code))  as iata_code,
        upper(trim(icao_code))  as icao_code,
        country,
        active
    from source
    where country = 'Russia'
      and active = true
      and iata_code is not null
)

select * from cleaned
