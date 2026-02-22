{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'intervention_key'
) }}

WITH interventions_source AS (
    SELECT * FROM {{ source('staging', 'interventions') }}
),

interventions AS (
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
    FROM interventions_source
)

SELECT * FROM interventions