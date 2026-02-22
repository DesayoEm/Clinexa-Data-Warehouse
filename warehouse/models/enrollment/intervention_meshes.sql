{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'mesh_key'
) }}

WITH intervention_meshes_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_meshes') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT mesh_key FROM {{ this }})
    {% endif %}
    ),

    intervention_meshes AS (
        SELECT
            mesh_key,
            mesh_id,
            mesh_term,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_meshes_source
    )

SELECT * FROM intervention_meshes