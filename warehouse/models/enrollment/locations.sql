{{ config(
    materialized='table',
    schema = 'enrollment'
) }}


-- Materialized as table (full refresh) rather than incremental.
-- ClinicalTrials.gov does not assign stable identifiers to location entities.
-- locations can be modified or deleted at the source, and since the primary key
-- is derived from location attributes, changes appear as new records rather than
-- updates. A full refresh ensures the API always serves current location state
-- without the risk of surfacing stale or deleted locations.


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
            CURRENT_DATE AS dbt_created_on
        FROM locations_source
    )

SELECT * FROM locations