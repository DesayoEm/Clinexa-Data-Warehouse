{{ config(
    materialized='table',
    schema = 'enrollment',
) }}


-- Materialized as table
-- Although collaborator entities themselves are stable, the relationship between
-- a study and its collaborators can change -- collaborators can be added or removed
-- over the course of a study. Since this is a junction table representing
-- current study-collaborator relationships, a full refresh ensures the API
-- always reflects the current state. Incremental would risk retaining
-- stale relationships that no longer exist at the source.


WITH study_collaborators_source AS (
    SELECT * FROM {{ source('staging', 'study_collaborators') }}
    ),

    study_collaborators AS (
        SELECT
            study_key,
            collaborator_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_collaborators_source
    )

SELECT * FROM study_collaborators