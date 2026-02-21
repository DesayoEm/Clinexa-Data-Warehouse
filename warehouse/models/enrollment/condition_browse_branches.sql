{{ config(
    materialized='table'
) }}

with condition_browse_branches_source as (
    select * from {{ source('staging', 'condition_browse_branches') }}
),

condition_browse_branches as (
    select
        *
    from condition_browse_branches_source
)

select * from condition_browse_branches