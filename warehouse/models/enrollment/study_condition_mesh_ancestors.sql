{{ config(
    materialized='table'
) }}

with study_condition_mesh_ancestors_source as (
    select * from {{ source('staging', 'study_condition_mesh_ancestors') }}
),

study_condition_mesh_ancestors as (
    select
        *
    from study_condition_mesh_ancestors_source
)

select * from study_condition_mesh_ancestors