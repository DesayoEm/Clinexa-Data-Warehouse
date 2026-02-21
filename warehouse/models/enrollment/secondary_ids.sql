{{ config(
    materialized='table'
) }}

with collaborators_source as (
    select * from {{ source('staging', 'collaborators') }}
),

collaborators as (
    select
        *
    from collaborators_source
)

select * from collaborators