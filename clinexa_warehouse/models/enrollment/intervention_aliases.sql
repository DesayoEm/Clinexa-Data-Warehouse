{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['intervention_key', 'intervention_alias_key']
) }}


WITH intervention_aliases_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_aliases') }}
    {% if is_incremental() %}
        WHERE intervention_key NOT IN (SELECT intervention_key FROM {{ this }})
        OR
        WHERE intervention_alias_key NOT IN (SELECT intervention_alias_key FROM {{ this }})

    {% endif %}
    ),

    intervention_aliases AS (
        SELECT
            intervention_alias_key
            intervention_key,
            intervention_name,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_aliases_source
    )



SELECT * FROM intervention_aliases