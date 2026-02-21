{{ config(
    materialized='table'
) }}

with locations_source as (
    select * from {{ source('staging', 'locations') }}
),

locations as (
    select
        *
    from locations_source
)

select * from locations