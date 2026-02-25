{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'condition_key'
) }}


WITH conditions_source AS (
    SELECT *
    FROM {{ source('staging', 'conditions') }}
    {% if is_incremental() %}
        WHERE condition_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),

    condition_meshes_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_meshes') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),

    condition_mesh_ancestors_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_mesh_ancestors') }}
    {% if is_incremental() %}
        WHERE ancestor_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),

    condition_browse_branches_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_browse_branches') }}
    {% if is_incremental() %}
        WHERE branch_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),

    condition_browse_leaves_source AS (
    SELECT *
    FROM {{ source('staging', 'condition_browse_leaves') }}
    {% if is_incremental() %}
        WHERE leaf_key NOT IN (SELECT condition_key FROM {{ this }})
    {% endif %}
    ),

    conditions AS (
        SELECT
            condition_key,
            NULL AS mesh_id,
            condition_name AS condition,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM conditions_source
    ),

    condition_meshes AS (
        SELECT
            mesh_key AS condition_key,
            mesh_id,
            mesh_term AS condition,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_meshes_source
    ),

    condition_mesh_ancestors AS (
        SELECT
            ancestor_key AS condition_key,
            ancestor_id AS mesh_id,
            ancestor_term AS condition,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_mesh_ancestors_source
    ),

    condition_browse_leaves AS (
        SELECT
            leaf_key AS condition_key,
            leaf_id AS mesh_id,
            name AS condition,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_browse_leaves_source
    ),

    condition_browse_branches AS (
        SELECT
            branch_key AS condition_key,
            NULL AS mesh_id,
            name AS condition,
            abbrev AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM condition_browse_branches_source
    ),


    FINAL AS (
        SELECT * FROM conditions
        UNION
        SELECT * FROM condition_meshes
        UNION
        SELECT * FROM condition_mesh_ancestors
        UNION
        SELECT * FROM condition_browse_leaves
        UNION
        SELECT * FROM condition_browse_branches
    )
SELECT * FROM final

