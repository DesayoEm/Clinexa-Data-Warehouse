{{ config(
    materialized = 'incremental',
    schema = 'outcome',
    unique_key = 'arm_group_key'
)}}


WITH arm_groups_source AS (
    SELECT *
    FROM {{ source ('staging', 'arm_groups')}})
    {% if is_incremental() %}
        WHERE arm_group_key NOT IN (SELECT arm_group_key FROM {{ this }})
    {% endif %}
    ),

    final AS (
        SELECT
            arm_group_key,
            study_key,
            arm_label,
            arm_description,
            arm_type,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM arm_groups_source
    ),

SELECT * FROM final;

