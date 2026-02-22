{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'location_key'
) }}

WITH locations_source AS (
    SELECT * FROM {{ source('staging', 'locations') }}
),

locations AS (
    SELECT
        location_key,
        facility,
        city,
        state,
        country,
        lat,
        lon,
        dag_execution_date,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on
    FROM locations_source
)

SELECT * FROM locations