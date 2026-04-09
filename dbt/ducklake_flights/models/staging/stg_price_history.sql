with source as (
    select * from {{ source('raw', 'price_history') }}
),

cleaned as (
    select
        price_id,
        flight_id,
        fare_class,
        cast(price_rub as decimal(10, 2)) as price_rub,
        recorded_at::timestamp            as recorded_at,
        days_before_departure
    from source
    where price_rub > 0
      and flight_id is not null
      and days_before_departure >= 0
)

select * from cleaned
