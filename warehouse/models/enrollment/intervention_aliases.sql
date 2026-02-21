{{ config(
    materialized='table'
) }}

with intervention_aliases_source as (
    select * from {{ source('staging', 'intervention_aliases') }}
),

intervention_aliases as (
    select
        *
    from intervention_aliases_source
)

select * from intervention_aliases