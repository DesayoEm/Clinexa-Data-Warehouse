{{ config(
    materialized='table'
) }}

with study_locations_source as (
    select * from {{ source('staging', 'study_locations') }}
),

study_locations as (
    select
        *
    from study_locations_source
)

select * from study_locations