{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'sponsor_key'
) }}


-- Materialized as incremental with a simple existence check on the unique key.
-- sponsors consist only of a name and class, neither of which changes once recorded.
-- The sponsor_key is a hash of these attributes, so duplicate records produce
-- identical keys and are ignored by the merge. 


WITH sponsors_source AS (
    SELECT *
    FROM {{ source('staging', 'sponsors') }}
    {% if is_incremental() %}
        WHERE sponsor_key NOT IN (SELECT sponsor_key FROM {{ this }})
    {% endif %}
    ),

    collaborators_source AS (
    SELECT *
    FROM {{ source('staging', 'collaborators') }}
    {% if is_incremental() %}
        WHERE collaborator_key NOT IN (SELECT collaborator_key FROM {{ this }})
    {% endif %}
    ),

    sponsors AS (
        SELECT
            sponsor_key,
            name,
            sponsor_class,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE as dbt_created_on
        FROM sponsors_source
    ),

    collaborators AS (
        SELECT
            collaborator_key as sponsor_key,
            name,
            collaborator_class as sponsor_class,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM collaborators_source
    ),

    final AS(
    SELECT * FROM sponsors
    UNION
    SELECT * FROM collaborators
)

SELECT * FROM final