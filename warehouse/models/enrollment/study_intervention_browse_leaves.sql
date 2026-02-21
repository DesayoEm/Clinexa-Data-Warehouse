{{ config(
    materialized='table'
) }}

with study_intervention_browse_leaves_source as (
    select * from {{ source('staging', 'study_intervention_browse_leaves') }}
),

study_intervention_browse_leaves as (
    select
        *
    from study_intervention_browse_leaves_source
)

select * from study_intervention_browse_leaves