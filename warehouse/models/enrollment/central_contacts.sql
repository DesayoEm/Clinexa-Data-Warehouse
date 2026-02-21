{{ config(
    materialized='table'
) }}

with central_contacts_source as (
    select * from {{ source('staging', 'central_contacts') }}
),

central_contacts as (
    select
        *
    from central_contacts_source
)

select * from central_contacts