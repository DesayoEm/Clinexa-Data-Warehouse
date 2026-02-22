{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'collaborator_key'
) }}

-- Materialized as incremental with a simple existence check on the unique key.
-- Collaborators consist only of a name and class, neither of which changes once recorded.
-- The collaborator_key is a hash of these attributes, so duplicate records produce
-- identical keys and are ignored by the merge.


WITH collaborators_source AS (
    SELECT *
    FROM {{ source('staging', 'collaborators') }}
    {% if is_incremental() %}
        WHERE collaborator_key NOT IN (SELECT collaborator_key FROM {{ this }})
    {% endif %}
),

collaborators AS (
    SELECT
        collaborator_key,
        name,
        collaborator_class,
        dag_execution_date,
        dag_id,
        dag_run_id,
        CURRENT_DATE AS dbt_created_on
    FROM collaborators_source
)

SELECT * FROM collaborators

WHERE last_seen_on > (SELECT MAX(last_seen_on) FROM {{ this }})
AND row_hash NOT IN (SELECT row_hash FROM {{ this }})