--- Interventions, their aliases, and their meshes at every level have the same value in the outcome layer,
--- and therefore belong in the same table


{{ config(
    materialized='table',
    schema = 'outcome'
    )
}}

WITH study_interventions_source AS (
    SELECT * FROM {{ source('staging', 'study_interventions') }}
    ),

     SELECT * FROM {{ source('staging', 'study_intervention_aliases') }}
    ),

    study_intervention_meshes_source AS (
        SELECT * FROM {{ source('staging', 'study_intervention_meshes') }}
    ),

    study_intervention_browse_branches_source AS (
        SELECT * FROM {{ source('staging', 'study_intervention_browse_branches') }}
    ),

    study_intervention_browse_leaves_source AS (
        SELECT * FROM {{ source('staging', 'study_intervention_browse_leaves') }}
    ),

    study_intervention_mesh_ancestors_source AS (
        SELECT * FROM {{ source('staging', 'study_intervention_mesh_ancestors') }}
),

    study_interventions AS (
        SELECT
            study_key,
            intervention_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_interventions_source
    ),

    study_intervention_meshes AS (
        SELECT
            study_key,
            mesh_key AS intervention_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_meshes_source
    ),

    study_intervention_browse_branches AS (
        SELECT
            study_key,
            branch_key AS intervention_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_browse_branches_source
    ),

    study_intervention_browse_leaves AS (
        SELECT
            study_key,
            leaf_key AS intervention_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_browse_leaves_source
    ),

    study_intervention_mesh_ancestors AS (
        SELECT
            study_key,
            ancestor_key as intervention_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_mesh_ancestors_source
    ),

    final AS (
        SELECT * FROM study_interventions
        UNION
        SELECT * FROM study_intervention_meshes
        UNION
        SELECT * FROM study_intervention_browse_branches
        UNION
        SELECT * FROM study_intervention_browse_leaves
        UNION
        SELECT * FROM study_intervention_mesh_ancestors

    )

SELECT * FROM final