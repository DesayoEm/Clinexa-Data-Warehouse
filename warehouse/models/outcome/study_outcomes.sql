{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'condition_key'
) }}


WITH primary_outcomes_source AS (
    SELECT *
    FROM {{ source('staging', 'primary_outcomes') }}
    {% if is_incremental() %}
        WHERE outcome_key NOT IN (SELECT outcome_key FROM {{ this }})
    {% endif %}
    ),

    secondary_outcomes_source AS (
    SELECT *
    FROM {{ source('staging', 'secondary_outcomes') }}
    {% if is_incremental() %}
        WHERE outcome_key NOT IN (SELECT outcome_key FROM {{ this }})
    {% endif %}
    ),

    other_outcomes_source AS (
    SELECT *
    FROM {{ source('staging', 'other_outcomes') }}
    {% if is_incremental() %}
        WHERE outcome_key NOT IN (SELECT outcome_key FROM {{ this }})
    {% endif %}
    ),

    primary_outcomes AS (
        SELECT
            outcome_key,
            study_key,
            measure,
            description,
            time_frame,
            'PRIMARY' AS outcome_type,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM primary_outcomes_source
    ),

    secondary_outcomes AS (
        SELECT
            outcome_key,
            study_key,
            measure,
            description,
            time_frame,
            'SECONDARY' AS outcome_type,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM secondary_outcomes_source
    ),

    other_outcomes AS (
        SELECT
            outcome_key,
            study_key,
            measure,
            description,
            time_frame,
            'OTHER_PRE_SPECIFIED' AS outcome_type,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM other_outcomes_source
    ),

    final AS (
        SELECT * FROM primary_outcomes
        UNION
        SELECT * FROM secondary_outcomes
        UNION
        SELECT * FROM other_outcomes
    )

SELECT * FROM final

