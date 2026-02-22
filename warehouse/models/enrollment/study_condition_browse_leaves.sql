{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'study_key']
) }}

WITH study_condition_browse_leaves_source AS (
    SELECT * FROM {{ source('staging', 'study_condition_browse_leaves') }}
),

study_condition_browse_leaves AS (
    SELECT
        study_key,
        leaf_key,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_condition_browse_leaves_source
)

SELECT * FROM study_condition_browse_leaves