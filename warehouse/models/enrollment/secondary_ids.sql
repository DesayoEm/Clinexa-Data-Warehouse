{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'secondary_id_key'
) }}

WITH secondary_ids_source AS (
    SELECT * FROM {{ source('staging', 'secondary_ids') }}
),

secondary_ids AS (
    SELECT
        secondary_id_key,
        study_key,
        id,
        type,
        domain,
        link,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_TIMESTAMP AS dbt_created_on
    FROM secondary_ids_source
)

SELECT * FROM secondary_ids