{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'branch_key']
) }}

WITH study_intervention_browse_branches_source AS (
    SELECT * FROM {{ source('staging', 'study_intervention_browse_branches') }}
),

study_intervention_browse_branches AS (
    SELECT
        branch_key,
        study_key,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_intervention_browse_branches_source
)

SELECT * FROM study_intervention_browse_branches