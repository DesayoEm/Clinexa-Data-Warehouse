{{ config(
    materialized='table'
) }}

with interventions_source as (
    select * from {{ source('staging', 'interventions') }}
),

interventions as (
    select
        *
    from interventions_source
)

select * from interventions