{{ config(
    materialized='table',
    schema = 'enrollment'
) }}



-- Materialized as table (full refresh) rather than incremental.
-- ClinicalTrials.gov does not assign stable identifiers to contact entities.
-- Contacts can be modified or deleted at the source, and since the primary key
-- is derived from contact attributes, changes appear as new records rather than
-- updates. A full refresh ensures the API always serves current contact state
-- without the risk of surfacing stale or deleted contacts.


WITH
    location_contacts_source AS (
        SELECT * FROM {{ source('staging', 'location_contacts') }}
    ),

    central_contacts_source AS (
        SELECT * FROM {{ source('staging', 'central_contacts') }}
    ),

    location_contacts AS (
        SELECT
            contact_key,
            name,
            role,
            phone,
            phone_ext,
            email,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM location_contacts_source
    ),

    central_contacts AS (
        SELECT
            contact_key,
            name,
            role,
            phone,
            phone_ext,
            email,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM central_contacts_source
    ),


    final AS(
        SELECT * FROM location_contacts
        UNION
        SELECT * FROM central_contacts
    )


SELECT * FROM final


