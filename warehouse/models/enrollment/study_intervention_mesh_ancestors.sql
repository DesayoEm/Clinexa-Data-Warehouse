{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'ancestor_key']
) }}


WITH study_intervention_mesh_ancestors_source AS (
    SELECT * FROM {{ source('staging', 'study_intervention_mesh_ancestors') }}
),

study_intervention_mesh_ancestors AS (
    SELECT
        ancestor_key,
        study_key,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_intervention_mesh_ancestors_source
)

SELECT * FROM study_intervention_mesh_ancestors