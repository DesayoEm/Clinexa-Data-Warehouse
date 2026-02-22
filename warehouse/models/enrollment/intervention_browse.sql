{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'term_key'
) }}

WITH intervention_meshes_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_meshes') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    intervention_mesh_ancestors_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_mesh_ancestors') }}
    {% if is_incremental() %}
        WHERE ancestor_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    intervention_browse_leaves_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_browse_leaves') }}
    {% if is_incremental() %}
        WHERE leaf_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    intervention_browse_branches_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_browse_branches') }}
    {% if is_incremental() %}
        WHERE branch_key NOT IN (SELECT term_key FROM {{ this }})
    {% endif %}
    ),

    intervention_meshes AS (
        SELECT
            mesh_key AS term_key,
            mesh_id AS term_id,
            mesh_term AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_meshes_source
    ),
    
    intervention_mesh_ancestors AS (
        SELECT
            ancestor_key AS term_key,
            ancestor_id AS term_id,
            ancestor_term AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_mesh_ancestors_source
    ),

    intervention_browse_leaves AS (
        SELECT
            leaf_key AS term_key,
            leaf_id AS term_id,
            name AS term,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_browse_leaves_source
    ),

    intervention_browse_branches AS (
        SELECT
            branch_key AS term_key,
            NULL AS term_id,
            name AS term,
            abbrev AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_browse_branches_source
    ),


    FINAL AS (
        SELECT * FROM intervention_meshes
        UNION
        SELECT * FROM intervention_mesh_ancestors
        UNION
        SELECT * FROM intervention_browse_leaves
        UNION
        SELECT * FROM intervention_browse_branches
    )

SELECT * FROM final