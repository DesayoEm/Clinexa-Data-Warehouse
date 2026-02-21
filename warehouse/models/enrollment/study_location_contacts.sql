{{ config(
    materialized='table'
) }}

with study_location_contacts_source as (
    select * from {{ source('staging', 'study_location_contacts') }}
),

study_location_contacts as (
    select
        *
    from study_location_contacts_source
)

select * from study_location_contacts