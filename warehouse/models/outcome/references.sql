{{ config(
    materialized='incremental',
    schema = 'outcome',
    unique_key = 'ref_key'
) }}


WITH study_ref_source AS (
    SELECT *
    FROM {{ source('staging', 'study_references') }}
    {% if is_incremental() %}
        WHERE ref_key NOT IN (SELECT ref_key FROM {{ this }})
    {% endif %}
    ),

    link_ref_source AS (
    SELECT *
    FROM {{ source('staging', 'link_references') }}
    {% if is_incremental() %}
        WHERE ref_key NOT IN (SELECT ref_key FROM {{ this }})
    {% endif %}
    ),

    ipd_ref_source AS (
    SELECT *
    FROM {{ source('staging', 'ipd_references') }}
    {% if is_incremental() %}
        WHERE ref_key NOT IN (SELECT ref_key FROM {{ this }})
    {% endif %}
    ),


    study_ref AS (
        SELECT
            ref_key,
            study_key,
            "STUDY" AS ref_type,
            type AS study_ref_type,
            NULL AS ipd_ref_type,
            NULL AS label,
            NULL AS url,
            pmid,
            citation,
            NULL AS comment,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM study_ref_source
    ),

    link_refs AS (
         SELECT
            link_key as ref_key,
            study_key,
            "LINK" AS ref_type,
            NULL AS study_ref_type,
            NULL AS ipd_ref_type,
            label,
            url,
            NULL AS pmid,
            NULL AS citation,
            NULL AS comment,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM link_ref_source
    ),

    ipd_refs AS (
         SELECT
            ipd_key as ref_key,
            study_key,
            "IPD" AS ref_type,
            NULL AS study_ref_type,
            type AS ipd_ref_type,
            id AS label,
            url,
            NULL AS pmid,
            NULL AS citation,
            comment,
            dag_execution_date,
            dag_id,
            dag_run_id,
            CURRENT_DATE AS dbt_created_on
        FROM ipd_ref_source
    ),

    final AS (
        SELECT * FROM study_ref
        UNION
        SELECT * FROM link_refs
        UNION
        SELECT * FROM ipd_refs
    )
SELECT * FROM final

