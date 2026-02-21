{{ config(
    materialized='table'
) }}

with conditions_source as (
    select * from {{ source('staging', 'conditions') }}
),

conditions as (
    select
        *
    from conditions_source
)

select * from conditions