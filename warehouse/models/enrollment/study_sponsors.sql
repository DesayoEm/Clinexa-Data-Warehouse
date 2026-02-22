{{ config(
    materialized='table',
    schema = 'enrollment'
) }}


-- Materialized as table
-- Although sponsor entities themselves are stable, the relationship between
-- a study and its sponsors can change -- sponsors can be added or removed
-- over the course of a study. Since this is a junction table representing
-- current study-sponsor relationships, a full refresh ensures the API
-- always reflects the current state. Incremental would risk retaining
-- stale relationships that no longer exist at the source.



WITH study_sponsors_source AS (
    SELECT * FROM {{ source('staging', 'study_sponsors') }}
    ),

    study_sponsors AS (
        SELECT
            study_key,
            sponsor_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_sponsors_source
    )

SELECT * FROM study_sponsors