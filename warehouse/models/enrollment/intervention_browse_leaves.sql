{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'leaf_key'
) }}


WITH intervention_browse_leaves_source AS (
    SELECT * FROM {{ source('staging', 'intervention_browse_leaves') }}
),

intervention_browse_leaves AS (
    SELECT
        leaf_key,
        leaf_id,
        name,
        as_found,
        relevance,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM intervention_browse_leaves_source
)

SELECT * FROM intervention_browse_leaves