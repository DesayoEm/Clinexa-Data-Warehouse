{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'branch_key'
) }}

WITH condition_browse_branches_source AS (
    SELECT * FROM {{ source('staging', 'condition_browse_branches') }}
),

condition_browse_branches AS (
    SELECT
        branch_key,
        abbrev,
        name,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM condition_browse_branches_source
)

SELECT * FROM condition_browse_branches