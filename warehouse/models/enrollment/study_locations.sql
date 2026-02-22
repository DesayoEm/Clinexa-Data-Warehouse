{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'location_key']
) }}




WITH study_locations_source AS (
    SELECT * FROM {{ source('staging', 'study_locations') }}
),

study_locations AS (
    SELECT
        study_key,
        location_key,
        status,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM study_locations_source
)

SELECT * FROM study_locations