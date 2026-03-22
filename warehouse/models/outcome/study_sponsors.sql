{{ config(
    materialized='table',
    schema = 'enrollment'
) }}


-- Materialized as table

-- Although study-sponsor relationships can change over the course of a study
-- (sponsors added, removed), this model is scoped to trial
-- outcomes only. Sponsor relationship history is not an outcomes attribute and belongs a in the landscape model.

-- Current sponsor state is sufficient here.


WITH study_sponsors_source AS (
    SELECT * FROM {{ source('staging', 'study_sponsors') }}
    ),

    study_collaborators_source AS (
        SELECT * FROM {{ source('staging', 'study_collaborators') }}
    ),

    study_sponsors AS (
        SELECT
            study_key,
            sponsor_key,
            True AS is_lead,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_sponsors_source
    ),

    study_collaborators AS (
        SELECT
            study_key,
            collaborator_key,
            False AS is_lead,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_collaborators_source
    ),

        final AS(
        SELECT * FROM study_sponsors
        UNION
        SELECT * FROM study_collaborators
    )


SELECT * FROM final