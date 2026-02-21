{{ config(
    materialized='table'
) }}

with intervention_browse_branches_source as (
    select * from {{ source('staging', 'intervention_browse_branches') }}
),

intervention_browse_branches as (
    select
        *
    from intervention_browse_branches_source
)

select * from intervention_browse_branches