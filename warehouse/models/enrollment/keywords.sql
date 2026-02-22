{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'keyword_key'
) }}

WITH keywords_source AS (
    SELECT * FROM {{ source('staging', 'keywords') }}
),

keywords AS (
    SELECT
        keyword_key,
        keyword_name,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM keywords_source
)

SELECT * FROM keywords