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

sponsors AS (
    SELECT
        sponsor_key CHAR(16) PRIMARY KEY,
        name TEXT,
        sponsor_class TEXT,
        dag_execution_date DATE,
        dag_id VARCHAR(100),
        dag_run_id VARCHAR(100),
        first_loaded_on DATE,
        last_seen_on DATE,
        CURRENT_DATE as dbt_created_on
    FROM sponsors_source
)

SELECT * FROM sponsors