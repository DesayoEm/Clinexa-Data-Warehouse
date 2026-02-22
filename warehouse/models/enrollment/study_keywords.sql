{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'keyword_key']
) }}


WITH study_keywords_source AS (
    SELECT * FROM {{ source('staging', 'study_keywords') }}
),

study_keywords AS (
    SELECT
        study_key,
        keyword_key,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_keywords_source
)

SELECT * FROM study_keywords