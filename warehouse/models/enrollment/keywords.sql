{{ config(
    materialized='table'
) }}

with keywords_source as (
    select * from {{ source('staging', 'keywords') }}
),

keywords as (
    select
        *
    from keywords_source
)

select * from keywords