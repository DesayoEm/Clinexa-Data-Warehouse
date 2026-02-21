{{ config(
    materialized='table'
) }}

with study_condition_browse_branches_source as (
    select * from {{ source('staging', 'study_condition_browse_branches') }}
),

study_condition_browse_branches as (
    select
        *
    from study_condition_browse_branches_source
)

select * from study_condition_browse_branches