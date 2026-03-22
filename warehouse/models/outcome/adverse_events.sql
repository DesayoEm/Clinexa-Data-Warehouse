{{ config(
    materialized = 'incremental',
    schema = 'outcome',
    unique_key = 'adverse_event_key'
)}}


WITH adverse_events_source AS (
    SELECT *
    FROM {{ source ('staging', 'adverse_events')}})
    {% if is_incremental() %}
        WHERE adverse_event_key NOT IN (SELECT adverse_event_key FROM {{ this }})
    {% endif %}
    ),

    adverse_events AS (
        SELECT
            adverse_event_key,
            study_key,
            description,
            frequency_threshold,
            time_frame,
            mortality_cmt,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM adverse_events_source
    )

SELECT * FROM adverse_events;

