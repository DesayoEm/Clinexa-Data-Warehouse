{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'intervention_key'
) }}

WITH intervention_aliases_source AS (
    SELECT * FROM {{ source('staging', 'intervention_aliases') }}
),

intervention_aliases AS (
    SELECT
        intervention_key,
        intervention_name,
        intervention_type,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM intervention_aliases_source
)

SELECT * FROM intervention_aliases