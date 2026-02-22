{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'ancestor_key'
) }}

WITH condition_mesh_ancestors_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_mesh_ancestors') }}
    {% if is_incremental() %}
        WHERE ancestor_key NOT IN (SELECT ancestor_key FROM {{ this }})
    {% endif %}
    ),

    condition_mesh_ancestors AS (
        SELECT
            ancestor_key,
            ancestor_id,
            ancestor_term,
            term,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_mesh_ancestors_source
    )

SELECT * FROM condition_mesh_ancestors