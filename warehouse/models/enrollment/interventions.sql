{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'intervention_key'
) }}

WITH intervention_aliases_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_aliases') }}
    {% if is_incremental() %}
        WHERE intervention_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    interventions_source AS (
    SELECT *
    FROM {{ source('staging', 'interventions') }}
    {% if is_incremental() %}
        WHERE intervention_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_aliases AS (
        SELECT
            intervention_key,
            intervention_name,
            intervention_type,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_aliases_source
    ),

    interventions AS (
    SELECT
        intervention_key,
        intervention_name,
        intervention_type,
        dag_execution_date,
        dag_id,
        dag_run_id,
        CURRENT_DATE AS dbt_created_on
    FROM interventions_source
    ),

    final AS(
    SELECT * FROM intervention_aliases
    UNION
    SELECT * FROM interventions
    )

SELECT * FROM final