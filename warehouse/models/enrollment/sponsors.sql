{{ config(
    materialized='table'
) }}

with sponsors_source as (
    select * from {{ source('staging', 'sponsors') }}
),

sponsors as (
    select
        *
    from sponsors_source
)

select * from sponsors