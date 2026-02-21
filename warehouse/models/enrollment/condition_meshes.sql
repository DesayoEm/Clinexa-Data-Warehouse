{{ config(
    materialized='table'
) }}

with condition_meshes_source as (
    select * from {{ source('staging', 'condition_meshes') }}
),

condition_meshes as (
    select
        *
    from condition_meshes_source
)

select * from condition_meshes