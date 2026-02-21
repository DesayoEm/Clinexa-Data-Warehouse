{{ config(
    materialized='table'
) }}

with condition_browse_leaves_source as (
    select * from {{ source('staging', 'condition_browse_leaves') }}
),

condition_browse_leaves as (
    select
        *
    from condition_browse_leaves_source
)

select * from condition_browse_leaves