{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'mesh_key'
) }}


WITH intervention_meshes_source AS (
    SELECT * FROM {{ source('staging', 'intervention_meshes') }}
),

intervention_meshes AS (
    SELECT
        mesh_key,
        mesh_id,
        mesh_term,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM intervention_meshes_source
)

SELECT * FROM intervention_meshes