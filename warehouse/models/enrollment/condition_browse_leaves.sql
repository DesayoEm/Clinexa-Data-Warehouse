{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'leaf_key'
) }}


WITH condition_browse_leaves_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_browse_leaves') }}
    {% if is_incremental() %}
        WHERE leaf_key NOT IN (SELECT leaf_key FROM {{ this }})
    {% endif %}
    ),

    condition_browse_leaves AS (
        SELECT
            leaf_key,
            leaf_id,
            name,
            as_found,
            relevance,
            dag_execution_date,
            dag_id,
            dag_run_id
            CURRENT_DATE AS dbt_created_on
        FROM condition_browse_leaves_source
    )

SELECT * FROM condition_browse_leaves