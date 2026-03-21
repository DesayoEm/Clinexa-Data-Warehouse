{{ config(
    materialized = 'incremental',
    schema = 'outcome',
    unique_key = 'outcome_measure_key'
)}}


WITH outcome_measures_source AS (
    SELECT *
    FROM {{ source ('staging', 'outcome_measures')}})
    {% if is_incremental() %}
        WHERE outcome_measure_key NOT IN (SELECT outcome_measure_key FROM {{ this }})
    {% endif %}
    ),

     outcome_measures AS (
        SELECT
            outcome_measure_key
            study_key,
            intervention_key,
            outcome_type,
            title,
            description TEXT,
            population_description,
            reporting_status,
            anticipated_posting_date,
            CASE
                WHEN anticipated_posting_date ~ '^\d{4}-\d{2}$'
                THEN (anticipated_posting_date || '-01') :: DATE
            END AS anticipated_posting_date_parsed,
            param_type,
            dispersion_type,
            unit_of_measure,
            calculate_pct,
            time_frame,
            denom_units_selected,
            type_units_analysed,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM outcome_measures_source
    )

SELECT * FROM outcome_measures;