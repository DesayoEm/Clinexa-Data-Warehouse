{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'id_alias_key'
) }}


WITH nct_aliases_source AS (
    SELECT * FROM {{ source('staging', 'nct_aliases') }}
),

nct_aliases AS (
    SELECT
        id_alias_key,
        study_key,
        id_alias,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE as dbt_created_on
    FROM nct_aliases_source
)

SELECT * FROM nct_aliases