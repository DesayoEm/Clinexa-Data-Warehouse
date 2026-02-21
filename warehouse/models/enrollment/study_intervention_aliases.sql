{{ config(
    materialized='table'
) }}

with study_intervention_aliases_source as (
    select * from {{ source('staging', 'study_intervention_aliases') }}
),

study_intervention_aliases as (
    select
        *
    from study_intervention_aliases_source
)

select * from study_intervention_aliases