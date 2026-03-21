{{ config(
    materialized = 'incremental',
    schema = 'outcome',
    unique_key = 'arm_intervention_key'
)}}


WITH arm_groups_source AS (
    SELECT *
    FROM {{ source ('staging', 'arm_interventions')}})
    {% if is_incremental() %}
        WHERE arm_intervention_key NOT IN (SELECT arm_intervention_key FROM {{ this }})
    {% endif %}
    ),

    final AS (
        SELECT
            arm_intervention_key,
            arm_group_key,
            study_key,
            arm_label,
            arm_intervention_name AS arm_intervention,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM arm_groups_source
    )

SELECT * FROM final;



---does this need to be type 2. model is not a periodic snapshot
