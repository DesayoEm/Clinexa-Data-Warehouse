{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'intervention_key']
) }}


WITH study_interventions_source AS (
    SELECT * FROM {{ source('staging', 'study_interventions') }}
),

study_interventions AS (
    SELECT
        study_key,
        intervention_key,
        description,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_interventions_source
)

SELECT * FROM study_interventions