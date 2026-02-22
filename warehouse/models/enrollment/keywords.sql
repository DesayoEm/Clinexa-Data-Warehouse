{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'keyword_key'
) }}


WITH keywords_source AS (
    SELECT *
    FROM {{ source('staging', 'keywords') }}
    {% if is_incremental() %}
        WHERE keyword_key NOT IN (SELECT keyword_key FROM {{ this }})
    {% endif %}
    ),

    keywords AS (
        SELECT
            keyword_key,
            keyword_name,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM keywords_source
    )

SELECT * FROM keywords