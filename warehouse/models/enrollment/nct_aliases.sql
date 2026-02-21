{{ config(
    materialized='table'
) }}

with nct_aliases_source as (
    select * from {{ source('staging', 'nct_aliases') }}
),

nct_aliases as (
    select
        *
    from nct_aliases_source
)

select * from nct_aliases