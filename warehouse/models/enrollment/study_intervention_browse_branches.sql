{{ config(
    materialized='table'
) }}

with study_intervention_browse_branches_source as (
    select * from {{ source('staging', 'study_intervention_browse_branches') }}
),

study_intervention_browse_branches as (
    select
        *
    from study_intervention_browse_branches_source
)

select * from study_intervention_browse_branches