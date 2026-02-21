{{ config(
    materialized='table'
) }}

with study_sponsors_source as (
    select * from {{ source('staging', 'study_sponsors') }}
),

study_sponsors as (
    select
        *
    from study_sponsors_source
)

select * from study_sponsors