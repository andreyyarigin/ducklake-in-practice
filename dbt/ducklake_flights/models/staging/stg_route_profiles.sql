{{
    config(
        materialized='table',
        database='flights',
        schema='main'
    )
}}

select
    src_iata,
    dst_iata,
    src_iata || '-' || dst_iata     as route_key,
    base_load_factor,
    price_tier,
    seasonality_type,
    competition_level,
    notes

from {{ source('raw', 'route_profiles') }}
where src_iata is not null
  and dst_iata is not null
  and base_load_factor between 0.0 and 1.0
