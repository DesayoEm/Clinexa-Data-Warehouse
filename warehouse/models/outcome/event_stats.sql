{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'stat_key'
) }}


WITH serious_event_stats_source AS (
    SELECT *
    FROM {{ source('staging', 'serious_event_stats') }}
    {% if is_incremental() %}
        WHERE stat_key NOT IN (SELECT stat_key FROM {{ this }})
    {% endif %}
    ),


    other_event_stats_source AS (
    SELECT *
    FROM {{ source('staging', 'other_event_stats') }}
    {% if is_incremental() %}
        WHERE stat_key NOT IN (SELECT stat_key FROM {{ this }})
    {% endif %}
    ),


    serious_event_stats AS (
        SELECT
            event_stat_key,
            adverse_event_key,
            serious_event_key AS event_key,
            study_key,
            group_key,
            group_id,
            num_events,
            num_affected,
            num_at_risk,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM serious_event_stats_source
    ),

    other_event_stats AS (
         SELECT
            event_stat_key,
            adverse_event_key,
            other_event_key AS event_key,
            study_key,
            group_key,
            group_id,
            num_events,
            num_affected,
            num_at_risk,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM other_event_stats_source
    ),



    final AS (
        SELECT * FROM serious_event_stats_source
        UNION
        SELECT * FROM other_event_stats_sources
    )
SELECT * FROM final

