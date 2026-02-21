{{ config(
    materialized='table'
) }}

with study_keywords_source as (
    select * from {{ source('staging', 'study_keywords') }}
),

study_keywords as (
    select
        *
    from study_keywords_source
)

select * from study_keywords