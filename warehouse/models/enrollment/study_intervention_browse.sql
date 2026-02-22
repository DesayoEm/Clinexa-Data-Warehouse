{{ config(
    materialized='table',
    schema = 'enrollment',
) }}


WITH study_intervention_meshes_source AS (
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
        
    study_intervention_meshes AS (
        SELECT
            mesh_key AS term_key,
            study_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_meshes_source
    ),
    
    study_intervention_browse_branches AS (
        SELECT
            branch_key AS term_key,
            study_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_browse_branches_source
    ),
    
    study_intervention_browse_leaves AS (
        SELECT
            leaf_key AS term_key,
            study_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_browse_leaves_source
    ),
    
    study_intervention_mesh_ancestors AS (
        SELECT
            ancestor_key as term_key,
            study_key,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_intervention_mesh_ancestors_source
    ),
    
final AS (
        SELECT * FROM study_intervention_meshes
        UNION
        SELECT * FROM study_intervention_browse_branches
        UNION
        SELECT * FROM study_intervention_browse_leaves
        UNION
        SELECT * FROM study_intervention_mesh_ancestors
       
    )

SELECT * FROM final