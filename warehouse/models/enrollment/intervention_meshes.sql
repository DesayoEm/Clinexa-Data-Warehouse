{{ config(
    materialized='table'
) }}

with intervention_meshes_source as (
    select * from {{ source('staging', 'intervention_meshes') }}
),

intervention_meshes as (
    select
        *
    from intervention_meshes_source
)

select * from intervention_meshes