{{ config(
    materialized='table'
) }}

with intervention_mesh_ancestors_source as (
    select * from {{ source('staging', 'intervention_mesh_ancestors') }}
),

intervention_mesh_ancestors as (
    select
        *
    from intervention_mesh_ancestors_source
)

select * from intervention_mesh_ancestors