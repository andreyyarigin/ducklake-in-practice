-- stg_passengers: фильтрация и нормализация.
-- UUID-ключи гарантируют уникальность passenger_id из генератора.
select
    passenger_id,
    trim(first_name)      as first_name,
    trim(last_name)       as last_name,
    lower(trim(email))    as email,
    phone,
    date_of_birth::date   as date_of_birth,
    frequent_flyer_id,
    created_at::timestamp as created_at
from {{ source('raw', 'passengers') }}
where passenger_id is not null
