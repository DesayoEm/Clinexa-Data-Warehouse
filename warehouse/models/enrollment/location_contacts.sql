{{ config(
    materialized='table'
) }}

with location_contacts_source as (
    select * from {{ source('staging', 'location_contacts') }}
),

location_contacts as (
    select
        *
    from location_contacts_source
)

select * from location_contacts