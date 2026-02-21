{{ config(
    materialized='table'
) }}

with study_conditions_source as (
    select * from {{ source('staging', 'study_conditions') }}
),

study_conditions as (
    select
        *
    from study_conditions_source
)

select * from study_conditions