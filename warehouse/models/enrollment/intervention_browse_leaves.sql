{{ config(
    materialized='table'
) }}

with intervention_browse_leaves_source as (
    select * from {{ source('staging', 'intervention_browse_leaves') }}
),

intervention_browse_leaves as (
    select
        *
    from intervention_browse_leaves_source
)

select * from intervention_browse_leaves