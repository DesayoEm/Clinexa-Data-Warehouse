{{ config(
    materialized='table'
) }}

with study_collaborators_source as (
    select * from {{ source('staging', 'study_collaborators') }}
),

study_collaborators as (
    select
        *
    from study_collaborators_source
)

select * from study_collaborators