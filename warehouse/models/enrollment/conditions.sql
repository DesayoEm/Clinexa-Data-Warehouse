{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'condition_key'
) }}


WITH conditions_source AS (
    SELECT *
    FROM {{ source('staging', 'conditions') }}
    {% if is_incremental() %}
        WHERE condition_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),


    conditions AS (
        SELECT
            condition_key,
            condition_name,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM conditions_source
    )

SELECT * FROM conditions