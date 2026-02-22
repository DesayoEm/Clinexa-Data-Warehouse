{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'branch_key'
) }}


WITH intervention_browse_branches_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_browse_branches') }}
    {% if is_incremental() %}
        WHERE branch_key NOT IN (SELECT branch_key FROM {{ this }})
    {% endif %}
    ),

    intervention_browse_branches AS (
        SELECT
            branch_key,
            abbrev,
            name,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_browse_branches_source
    )

SELECT * FROM intervention_browse_branches