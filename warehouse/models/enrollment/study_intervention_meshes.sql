{{ config(
    materialized='table'
) }}

with study_intervention_meshes_source as (
    select * from {{ source('staging', 'study_intervention_meshes') }}
),

study_intervention_meshes as (
    select
        *
    from study_intervention_meshes_source
)

select * from study_intervention_meshes