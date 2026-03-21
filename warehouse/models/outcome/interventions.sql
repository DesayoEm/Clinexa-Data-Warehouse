{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'intervention_key'
) }}


WITH interventions_source AS (
    SELECT *
    FROM {{ source('staging', 'interventions') }}
    {% if is_incremental() %}
        WHERE intervention_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_meshes_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_meshes') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_aliases_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_aliases') }}
    {% if is_incremental() %}
        WHERE mesh_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_mesh_ancestors_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_mesh_ancestors') }}
    {% if is_incremental() %}
        WHERE ancestor_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_browse_branches_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_browse_branches') }}
    {% if is_incremental() %}
        WHERE branch_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    intervention_browse_leaves_source AS (
    SELECT *
    FROM {{ source('staging', 'intervention_browse_leaves') }}
    {% if is_incremental() %}
        WHERE leaf_key NOT IN (SELECT intervention_key FROM {{ this }})
    {% endif %}
    ),

    interventions AS (
        SELECT
            intervention_key,
            NULL AS mesh_id,
            intervention_name AS intervention,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM interventions_source
    ),

    intervention_aliases AS (
        SELECT
            intervention_alias_key AS intervention_key,
            NULL AS mesh_id,
            description AS intervention,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_aliases_source
    ),

    intervention_meshes AS (
        SELECT
            mesh_key AS intervention_key,
            mesh_id,
            mesh_term AS intervention,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_meshes_source
    ),

    intervention_mesh_ancestors AS (
        SELECT
            ancestor_key AS intervention_key,
            ancestor_id AS mesh_id,
            ancestor_term AS intervention,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_mesh_ancestors_source
    ),

    intervention_browse_leaves AS (
        SELECT
            leaf_key AS intervention_key,
            leaf_id AS mesh_id,
            name AS intervention,
            NULL AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_browse_leaves_source
    ),

    intervention_browse_branches AS (
        SELECT
            branch_key AS intervention_key,
            NULL AS mesh_id,
            name AS intervention,
            abbrev AS abbreviation,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM intervention_browse_branches_source
    ),


    final AS (
        SELECT * FROM interventions
        UNION
        SELECT * FROM intervention_aliases
        UNION
        SELECT * FROM intervention_meshes
        UNION
        SELECT * FROM intervention_mesh_ancestors
        UNION
        SELECT * FROM intervention_browse_leaves
        UNION
        SELECT * FROM intervention_browse_branches
    )
SELECT * FROM final

