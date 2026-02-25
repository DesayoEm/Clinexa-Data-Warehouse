{{ config(
    materialized='table',
    schema = 'outcome'
    )
}}

WITH study_conditions_source AS (
        SELECT * FROM {{ source('staging', 'study_conditions') }}
    ),
    study_condition_meshes_source AS (
        SELECT * FROM {{ source('staging', 'study_condition_meshes') }}
    ),

    study_condition_browse_branches_source AS (
        SELECT * FROM {{ source('staging', 'study_condition_browse_branches') }}
    ),

    study_condition_browse_leaves_source AS (
        SELECT * FROM {{ source('staging', 'study_condition_browse_leaves') }}
    ),

    study_condition_mesh_ancestors_source AS (
        SELECT * FROM {{ source('staging', 'study_condition_mesh_ancestors') }}
),

    study_conditions AS (
        SELECT
            study_key,
            condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_conditions_source
    ),

    study_condition_meshes AS (
        SELECT
            study_key,
            mesh_key AS condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_condition_meshes_source
    ),

    study_condition_browse_branches AS (
        SELECT
            study_key,
            branch_key AS condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_condition_browse_branches_source
    ),

    study_condition_browse_leaves AS (
        SELECT
            study_key,
            leaf_key AS condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_condition_browse_leaves_source
    ),

    study_condition_mesh_ancestors AS (
        SELECT
            study_key,
            ancestor_key as condition_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_condition_mesh_ancestors_source
    ),

    final AS (
        SELECT * FROM study_conditions
        UNION
        SELECT * FROM study_condition_meshes
        UNION
        SELECT * FROM study_condition_browse_branches
        UNION
        SELECT * FROM study_condition_browse_leaves
        UNION
        SELECT * FROM study_condition_mesh_ancestors

    )

SELECT * FROM final