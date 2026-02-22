
{{ config(
    materialized='table',
    schema = 'enrollment'
) }}


-- Materialized as table 
-- The relationship between a study and its locations can change -- locations can be added or removed
-- over the course of a study. Since this is a junction table representing
-- current study-location relationships, a full refresh ensures the API
-- always reflects the current state. Incremental would risk retaining
-- stale relationships that no longer exist at the source.



WITH study_locations_source AS (
    SELECT * FROM {{ source('staging', 'study_locations') }}
),

study_locations AS (
    SELECT
        study_key,
        location_key,
        status,
        dag_execution_date,
        dag_id,
        dag_run_id,
        CURRENT_DATE AS dbt_created_on
    FROM study_locations_source
)

SELECT * FROM study_locations