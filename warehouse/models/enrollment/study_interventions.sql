{{ config(
    materialized='table'
) }}

with study_interventions_source as (
    select * from {{ source('staging', 'study_interventions') }}
),

study_interventions as (
    select
        *
    from study_interventions_source
)

select * from study_interventions