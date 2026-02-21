{{ config(
    materialized='table'
) }}

with study_intervention_mesh_ancestors_source as (
    select * from {{ source('staging', 'study_intervention_mesh_ancestors') }}
),

study_intervention_mesh_ancestors as (
    select
        *
    from study_intervention_mesh_ancestors_source
)

select * from study_intervention_mesh_ancestors