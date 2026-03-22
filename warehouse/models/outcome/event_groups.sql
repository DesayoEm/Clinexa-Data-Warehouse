{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'sponsor_key'
) }}


-- Materialized as incremental with a simple existence check on the unique key.
-- sponsors consist only of a name and class, neither of which changes once recorded.
-- The sponsor_key is a hash of these attributes, so duplicate records produce
-- identical keys and are ignored by the merge.


WITH event_group_source AS (
    SELECT *
    FROM {{ source('staging', 'event_groups') }}
    {% if is_incremental() %}
        WHERE event_group_key NOT IN (SELECT event_group_key FROM {{ this }})
    {% endif %}
    ),

    event_groups AS (
        SELECT
            event_group_key,
            study_key,
            adverse_event_key,
            group_id,
            title,
            description,
            num_deaths,
            num_deaths_at_risk,
            num_serious,
            num_serious_at_risk,
            num_other,
            num_other_at_risk,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE as dbt_created_on
        FROM event_group_source
    )

SELECT * FROM event_group_source