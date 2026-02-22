{{ config(
    materialized='table',
    schema = 'enrollment'
) }}


-- Materialized as table rather than incremental.
-- ClinicalTrials.gov does not assign stable identifiers to contact entities.
-- Contacts can be modified or deleted at the source, and since the primary key
-- is derived from contact attributes, changes appear as new records rather than
-- updates. A full refresh ensures the API always serves current contact state
-- without the risk of surfacing stale or deleted contacts.


WITH study_location_contacts_source AS(
    SELECT * FROM {{ source('staging', 'study_location_contacts') }}
    ),

    study_location_contacts AS (
        SELECT
            study_key,
            location_key,
            contact_key,
            dag_execution_date,
            dag_id,
            dag_run_id
            CURRENT_DATE AS dbt_created_on
        FROM study_location_contacts_source
    )

SELECT * FROM study_location_contacts