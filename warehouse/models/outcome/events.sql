{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'event_key'
) }}


WITH adverse_events_source AS (
    SELECT *
    FROM {{ source('staging', 'serious_events') }}
    {% if is_incremental() %}
        WHERE event_key NOT IN (SELECT event_key FROM {{ this }})
    {% endif %}
    ),

    other_events_source AS (
    SELECT *
    FROM {{ source('staging', 'other_events') }}
    {% if is_incremental() %}
        WHERE event_key NOT IN (SELECT event_key FROM {{ this }})
    {% endif %}
    ),

    serious_events AS (
         SELECT
            adverse_event_key  AS event_key,
            "SERIOUS" AS event_type,
            study_key,
            description,
            CAST frequency_threshold AS FLOAT,
            time_frame,
            mortality_cmt,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM serious_events_source
    ),

    other_events AS (
         SELECT
            adverse_event_key  AS event_key,
            "OTHER" AS event_type,
            study_key,
            description,
            CAST frequency_threshold AS FLOAT,
            time_frame,
            mortality_cmt,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM other_events_source
    ),



    final AS (
        SELECT * FROM serious_events
        UNION
        SELECT * FROM other_events
    )
SELECT * FROM final

