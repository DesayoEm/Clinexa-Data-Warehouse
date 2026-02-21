{{ config(
    materialized='table'
) }}

with condition_mesh_ancestors_source as (
    select * from {{ source('staging', 'condition_mesh_ancestors') }}
),

condition_mesh_ancestors as (
    select
        *
    from condition_mesh_ancestors_source
)

select * from condition_mesh_ancestors