{{ config(
    materialized='table'
) }}

with study_central_contacts_source as (
    select * from {{ source('staging', 'study_central_contacts') }}
),

study_central_contacts as (
    select
        *
    from study_central_contacts_source
)

select * from study_central_contacts