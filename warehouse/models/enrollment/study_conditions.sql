{{ config(
    materialized='table',
    schema = 'enrollment'
    )
}}

WITH study_conditions_source AS (
    SELECT * FROM {{ source('staging', 'study_conditions') }}
    ),

    study_conditions AS (
        SELECT
            study_key,
            condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_conditions_source
    )

SELECT * FROM study_conditions