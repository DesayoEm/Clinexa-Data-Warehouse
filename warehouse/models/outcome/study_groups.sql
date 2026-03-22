{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = ['study_key', 'group_key']
) }}


WITH flow_groups_source AS (
    SELECT *
    FROM {{ source('staging', 'flow_groups') }}
    {% if is_incremental() %}
        WHERE study_key NOT IN (SELECT study_key FROM {{ this }})
        OR
        WHERE group_key NOT IN (SELECT group_key FROM {{ this }})

    {% endif %}
    ),

    event_groups_source AS (
        SELECT *
        FROM {{ source('staging', 'event_groups') }}
        {% if is_incremental() %}
            WHERE study_key NOT IN (SELECT study_key FROM {{ this }})
            OR
            WHERE group_key NOT IN (SELECT group_key FROM {{ this }})
        {% endif %}
        ),

    outcome_measure_groups_source AS (
        SELECT *
        FROM {{ source('staging', 'outcome_measure_groups') }}
        {% if is_incremental() %}
            WHERE study_key NOT IN (SELECT study_key FROM {{ this }})
            OR
            WHERE group_key NOT IN (SELECT group_key FROM {{ this }})
        {% endif %}
        ),



    flow_groups AS (
        SELECT
            group_key,
            study_key,
            group_id,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM flow_groups_source
    ),
    event_groups AS (
        SELECT
            group_key,
            study_key,,
            group_id
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM event_groups_source
    ),
    outcome_measure_groups AS (
        SELECT
            group_key,
            study_key,
            group_id,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM outcome_measure_groups_source
    ),

        final AS(
        SELECT * FROM flow_groups
        UNION
        SELECT * FROM event_groups,
        UNION
        SELECT * FROM outcome_measure_groups
    )

SELECT * FROM final