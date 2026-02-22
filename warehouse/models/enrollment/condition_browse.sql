{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'term_key'
) }}

WITH condition_meshes_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_meshes') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    condition_mesh_ancestors_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_mesh_ancestors') }}
    {% if is_incremental() %}
        WHERE ancestor_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    condition_browse_branches_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_browse_branches') }}
    {% if is_incremental() %}
        WHERE branch_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    condition_browse_leaves_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_browse_leaves') }}
    {% if is_incremental() %}
        WHERE leaf_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    condition_meshes AS (
        SELECT
            mesh_key AS term_key,
            mesh_id AS term_id,
            mesh_term AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_meshes_source
    ),
    
    condition_mesh_ancestors AS (
        SELECT
            ancestor_key AS term_key,
            ancestor_id AS term_id,
            ancestor_term AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_mesh_ancestors_source
    ),

    condition_browse_leaves AS (
        SELECT
            leaf_key AS term_key,
            leaf_id AS term_id,
            name AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_browse_leaves_source
    ),

    condition_browse_branches AS (
        SELECT
            branch_key AS term_key,
            NULL AS term_id,
            name AS term,
            abbrev AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_browse_branches_source
    ),
    
    
    FINAL AS (
        SELECT * FROM condition_meshes
        UNION
        SELECT * FROM condition_mesh_ancestors
        UNION
        SELECT * FROM condition_browse_leaves
        UNION
        SELECT * FROM condition_browse_branches
    )
SELECT * FROM final