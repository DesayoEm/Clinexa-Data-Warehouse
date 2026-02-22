{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'id_alias_key'
) }}


WITH nct_aliases_source AS (
    SELECT *
    FROM {{ source('staging', 'nct_aliases') }}
    {% if is_incremental() %}
        WHERE id_alias_key NOT IN (SELECT id_alias_key FROM {{ this }})
    {% endif %}
    ),

    nct_aliases AS (
        SELECT
            id_alias_key,
            study_key,
            id_alias,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE as dbt_created_on
        FROM nct_aliases_source
    )

SELECT * FROM nct_aliases