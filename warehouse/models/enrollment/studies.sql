{{ config(
    materialized='incremental',
    schema = 'enrollment',
    unique_key = 'study_key'
) }}



with studies_source as (
    select * from {{ source ('staging', 'studies')}}
),

studies_transformed AS (
    SELECT
        study_key,
        nct_id,
        org_study_id,
        brief_title,
        official_title,
        acronym,
        brief_summary,
        detailed_desc,
        responsible_party_type,
        study_type,
        patient_registry,
        enrollment_type,
        CAST enrollment_count AS INT,
        design_allocation,
        design_intervention_model,
        design_intervention_model_desc,
        design_primary_purpose,
        design_masking,
        design_observational_model,
        design_time_perspective,
        biospec_retention,
        biospec_desc,
        design_masking,
        eligibility_criteria,
        healthy_volunteers,
        sex,
        min_age,
        max_age,
        CASE
            WHEN minimum_age_years >= 65 then 'Seniors'
            WHEN minimum_age_years >= 18 then 'Adults'
            WHEN maximum_age_years < 18 then 'Pediatric'
            ELSE 'All Ages'
        END AS age_group,
        population_desc,
        overall_status,
        CASE
            WHEN overall_status in ('RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION')
            THEN true ELSE false
        END AS is_active,
        last_known_status,
        status_verified_date,
        CASE
            WHEN start_date ~ '^\d{4}-\d{2}$'
            THEN (start_date || '-01')::DATE
        END AS status_verified_date_parsed,
        start_date,
        CASE
            WHEN start_date ~ '^\d{4}-\d{2}$'
            THEN (start_date || '-01')::DATE
        END AS start_date_parsed,
        start_date_type,
        completion_date,
        CASE
            WHEN completion_date ~ '^\d{4}-\d{2}$'
            THEN (completion_date || '-01')::DATE
        END AS completion_date_parsed,
        completion_date_type,
        has_expanded_access,
        expanded_access_nct,
        is_fda_regulated_drug,
        is_fda_regulated_device,
        is_unapproved_device,
        is_us_export,
        ipd_sharing,
        ipd_access_criteria,
        poc_title,
        poc_organization,
        poc_email,
        poc_phone,
        poc_phone_ext,
        last_updated,
        dag_execution_date DATE,
        dag_id,
        dag_run_id,
        first_loaded_on,
        last_seen_on,
        CURRENT_DATE AS dbt_created_on

    FROM studies_source
    )
SELECT * FROM studies_transformed

