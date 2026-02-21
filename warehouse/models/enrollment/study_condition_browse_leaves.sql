{{ config(
    materialized='table'
) }}

with study_condition_browse_leaves_source as (
    select * from {{ source('staging', 'study_condition_browse_leaves') }}
),

study_condition_browse_leaves as (
    select
        *
    from study_condition_browse_leaves_source
)

select * from study_condition_browse_leaves