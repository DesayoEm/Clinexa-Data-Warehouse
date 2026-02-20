CREATE SCHEMA staging;
CREATE SCHEMA dev;
CREATE SCHEMA landscape_patient_matching;
CREATE SCHEMA outcome_analysis;

-- ENUMS
CREATE TYPE ResponsiblePartyType AS ENUM(
    'SPONSOR',
    'PRINCIPAL_INVESTIGATOR',
    'SPONSOR_INVESTIGATOR'
);

CREATE TYPE StudyType AS ENUM(
    'EXPANDED_ACCESS',
    'INTERVENTIONAL',
    'OBSERVATIONAL'
);


CREATE TYPE DesignAllocation AS ENUM(
    'RANDOMIZED',
    'NON_RANDOMIZED',
    'N/A',
    'NA'
);

CREATE TYPE InterventionalAssignment AS ENUM(
    'SINGLE_GROUP',
    'PARALLEL',
    'CROSSOVER',
    'FACTORIAL',
    'SEQUENTIAL'
);

CREATE TYPE PrimaryPurpose AS ENUM(
    'TREATMENT',
    'PREVENTION',
    'DIAGNOSTIC',
    'ECT',
    'SUPPORTIVE_CARE',
    'SCREENING',
    'HEALTH_SERVICES_RESEARCH',
    'BASIC_SCIENCE',
    'DEVICE_FEASIBILITY',
    'OTHER'
);

CREATE TYPE ObservationalModel AS ENUM(
    'COHORT',
    'CASE_CONTROL',
    'CASE_ONLY',
    'CASE_CROSSOVER',
    'ECOLOGIC_OR_COMMUNITY',
    'FAMILY_BASED',
    'DEFINED_POPULATION',
    'NATURAL_HISTORY',
    'OTHER'
);

CREATE TYPE DesignTimePerspective AS ENUM(
    'RETROSPECTIVE',
    'PROSPECTIVE',
    'CROSS_SECTIONAL',
    'OTHER'
);

CREATE TYPE DesignMasking AS ENUM(
    'NONE',
    'SINGLE',
    'DOUBLE',
    'TRIPLE',
    'QUADRUPLE'
);

CREATE TYPE BioSpecRetention AS ENUM(
    'NONE_RETAINED',
    'SAMPLES_WITH_DNA',
    'SAMPLES_WITHOUT_DNA'
);

CREATE TYPE BioSex AS ENUM(
    'FEMALE',
    'MALE',
    'ALL'
);

CREATE TYPE SamplingMethod AS ENUM(
    'PROBABILITY_SAMPLE',
    'NON_PROBABILITY_SAMPLE'
);

CREATE TYPE Status AS ENUM(
    'ACTIVE_NOT_RECRUITING',
    'COMPLETED',
    'ENROLLING_BY_INVITATION',
    'NOT_YET_RECRUITING',
    'RECRUITING',
    'SUSPENDED',
    'TERMINATED',
    'WITHDRAWN',
    'AVAILABLE',
    'NO_LONGER_AVAILABLE',
    'TEMPORARILY_NOT_AVAILABLE',
    'APPROVED_FOR_MARKETING',
    'WITHHELD',
    'UNKNOWN'
);

CREATE TYPE RecruitmentStatus AS ENUM(
    'ACTIVE_NOT_RECRUITING',
    'COMPLETED',
    'ENROLLING_BY_INVITATION',
    'NOT_YET_RECRUITING',
    'RECRUITING',
    'SUSPENDED',
    'TERMINATED',
    'WITHDRAWN',
    'AVAILABLE'
);


CREATE TYPE DateType AS ENUM(
    'ACTUAL',
    'ESTIMATED'
);

CREATE TYPE ExpandedAccessStatus AS ENUM(
    'AVAILABLE',
    'NO_LONGER_AVAILABLE',
    'TEMPORARILY_NOT_AVAILABLE',
    'APPROVED_FOR_MARKETING'
);

CREATE TYPE IpdSharing AS ENUM(
    'YES',
    'NO',
    'UNDECIDED'
);

CREATE TYPE AgreementRestrictionType AS ENUM(
    'LTE60',
    'GT60',
    'OTHER'
);


CREATE TYPE ArmGroupType AS ENUM(
    'EXPERIMENTAL',
    'ACTIVE_COMPARATOR',
    'PLACEBO_COMPARATOR',
    'SHAM_COMPARATOR',
    'NO_INTERVENTION',
    'OTHER'
);

CREATE TYPE InterventionType AS ENUM(
    'BEHAVIORAL',
    'BIOLOGICAL',
    'COMBINATION_PRODUCT',
    'DEVICE',
    'DIAGNOSTIC_TEST',
    'DIETARY_SUPPLEMENT',
    'DRUG',
    'GENETIC',
    'PROCEDURE',
    'RADIATION',
    'OTHER'
);

CREATE TYPE ReferenceType AS ENUM(
    'BACKGROUND',
    'RESULT',
    'DERIVED'
);

CREATE TYPE OutcomeMeasureType AS ENUM(
    'PRIMARY',
    'SECONDARY',
    'OTHER_PRE_SPECIFIED',
    'POST_HOC'
);

CREATE TYPE ReportingStatus AS ENUM(
    'NOT_POSTED',
    'POSTED'
);

CREATE TYPE MeasureParam AS ENUM(
    'GEOMETRIC_MEAN',
    'GEOMETRIC_LEAST_SQUARES_MEAN',
    'LEAST_SQUARES_MEAN',
    'LOG_MEAN',
    'MEAN',
    'MEDIAN',
    'NUMBER',
    'COUNT_OF_PARTICIPANTS',
    'COUNT_OF_UNITS'
);


CREATE TYPE AnalysisDispersionType AS ENUM(
    'STANDARD_DEVIATION',
    'STANDARD_ERROR_OF_MEAN'
);

CREATE TYPE ConfidenceIntervalNumSides AS ENUM(
    'ONE_SIDED',
    'TWO_SIDED'
);

CREATE TYPE NonInferiorityType AS ENUM(
    'SUPERIORITY',
    'NON_INFERIORITY',
    'EQUIVALENCE',
    'OTHER',
    'NON_INFERIORITY_OR_EQUIVALENCE',
    'SUPERIORITY_OR_OTHER',
    'NON_INFERIORITY_OR_EQUIVALENCE_LEGACY',
    'SUPERIORITY_OR_OTHER_LEGACY'
);

CREATE TYPE EventAssessment AS ENUM(
    'NON_SYSTEMATIC_ASSESSMENT',
    'SYSTEMATIC_ASSESSMENT'
);

CREATE TYPE ViolationEventType AS ENUM(
    'VIOLATION_IDENTIFIED',
    'CORRECTION_CONFIRMED',
    'PENALTY_IMPOSED',
    'ISSUES_IN_LETTER_ADDRESSED_CONFIRMED'
);

CREATE TYPE BrowseLeafRelevance AS ENUM(
    'LOW',
    'HIGH'
);


CREATE TYPE ContactRole AS ENUM(
    'STUDY_CHAIR',
    'STUDY_DIRECTOR',
    'PRINCIPAL_INVESTIGATOR',
    'SUB_INVESTIGATOR',
    'CONTACT'
);
--- fixed character lengths are created using the registry docs as as a guide. TEXT for unreliable fields


CREATE TABLE IF NOT EXISTS staging.studies(
    study_key CHAR(16) PRIMARY KEY,
    nct_id VARCHAR(15) NOT NULL UNIQUE,
    org_study_id VARCHAR(30),
    brief_title VARCHAR(300),
    official_title VARCHAR(600),
    acronym VARCHAR(14),
    brief_summary TEXT,
    detailed_desc TEXT,

    -- Admin
    responsible_party_type ResponsiblePartyType,

    -- Study Classification
    study_type StudyType,
    patient_registry BOOLEAN,

    -- Enrollment
    enrollment_type TEXT,
    enrollment_count FLOAT, -- Ideally int but API returns a float

    -- Design - Interventional
    design_allocation DesignAllocation,
    design_intervention_model InterventionalAssignment,
    design_intervention_model_desc TEXT,
    design_primary_purpose PrimaryPurpose,
    design_masking DesignMasking,

    -- Design - Observational
    design_observational_model ObservationalModel,
    design_time_perspective DesignTimePerspective,

    -- Biospecimens
    biospec_retention BioSpecRetention,
    biospec_desc TEXT,

    -- Eligibility
    eligibility_criteria TEXT,
    healthy_volunteers BOOLEAN,
    sex BioSex,
    min_age VARCHAR(20),
    max_age VARCHAR(20),
    population_desc TEXT,
    sampling_method SamplingMethod,

    -- Status & Dates
    overall_status Status,
    last_known_status Status,
    status_verified_date TEXT, --PARTIAL DATE
    start_date TEXT, --partial date
    start_date_type DateType,
    first_submit_date DATE,
    last_update_submit_date DATE,
    completion_date TEXT, --partial date,
    completion_date_type DateType,
    why_stopped TEXT,

    -- Expanded Access
    has_expanded_access BOOLEAN,
    expanded_access_nct VARCHAR(15),
    expanded_access_status ExpandedAccessStatus,

    -- Oversight
    has_dmc BOOLEAN,
    is_fda_regulated_drug BOOLEAN,
    is_fda_regulated_device BOOLEAN,
    is_unapproved_device BOOLEAN,
    is_us_export BOOLEAN,

    -- Data Sharing
    ipd_sharing IpdSharing,
    ipd_desc TEXT,
    ipd_time_frame TEXT,
    ipd_access_criteria TEXT,
    ipd_url TEXT,

    -- Point of Contact
    poc_title TEXT,
    poc_organization TEXT,
    poc_email TEXT,
    poc_phone TEXT,
    poc_phone_ext TEXT,

    -- Participant Flow
    flow_pre_assignment_details TEXT,
    flow_recruitment_details TEXT,
    flow_type_units_analysed TEXT,

    -- Agreements
    certain_agreement_pi_sponsor_employee BOOLEAN,
    certain_agreement_restrictive BOOLEAN,
    certain_agreement_restriction_type AgreementRestrictionType,
    certain_agreement_other_details TEXT,

    -- Results & Tracking
    sub_tracking_estimated_results_date TEXT, --Partial date
    has_results BOOLEAN,
    limitations_desc TEXT,

    -- Metadata
    version_holder DATE,
    last_updated DATE,

    -- ETL Audit Cols
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- STUDIES INDEXES
CREATE INDEX idx_studies_overall_status ON staging.studies(overall_status);
CREATE INDEX idx_studies_study_type ON staging.studies(study_type);
CREATE INDEX idx_studies_last_updated ON staging.studies(last_updated);
CREATE INDEX idx_studies_execution_date ON staging.studies(dag_execution_date);


-- Secondary IDs
CREATE TABLE staging.secondary_ids (
    secondary_id_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    id VARCHAR(30),
    type TEXT,
    domain TEXT,
    link TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- NCT Aliases
CREATE TABLE staging.nct_aliases (
    id_alias_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    id_alias VARCHAR(15) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Lead Sponsor 
CREATE TABLE staging.sponsors (
    sponsor_key CHAR(16) PRIMARY KEY,
    name TEXT,
    sponsor_class TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Sponsor 
CREATE TABLE staging.study_sponsors (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    sponsor_key CHAR(16) NOT NULL REFERENCES staging.sponsors(sponsor_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, sponsor_key)
);

-- Collaborator 
CREATE TABLE staging.collaborators (
    collaborator_key CHAR(16) PRIMARY KEY,
    name VARCHAR(160),
    collaborator_class TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Collaborator 
CREATE TABLE staging.study_collaborators (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    collaborator_key CHAR(16) NOT NULL REFERENCES staging.collaborators(collaborator_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, collaborator_key)
);

-- Conditions
CREATE TABLE staging.conditions (
    condition_key CHAR(16) PRIMARY KEY,
    condition_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Condition 
CREATE TABLE staging.study_conditions (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    condition_key CHAR(16) NOT NULL REFERENCES staging.conditions(condition_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, condition_key)
);

-- Keywords 
CREATE TABLE staging.keywords (
    keyword_key CHAR(16) PRIMARY KEY,
    keyword_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Keyword 
CREATE TABLE staging.study_keywords (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    keyword_key CHAR(16) NOT NULL REFERENCES staging.keywords(keyword_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, keyword_key)
);

-- Arm Groups
CREATE TABLE staging.arm_groups (
    arm_group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    arm_label VARCHAR(100),
    arm_description TEXT,
    arm_type ArmGroupType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Interventions 
CREATE TABLE staging.interventions (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name TEXT,
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Arm-Intervention (bridge table linking arms to intervention names)
CREATE TABLE staging.arm_interventions (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    arm_group_key CHAR(16) NOT NULL REFERENCES staging.arm_groups(arm_group_key),
    arm_intervention_key CHAR(16) NOT NULL REFERENCES staging.interventions(intervention_key),
    arm_intervention_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, arm_group_key, arm_intervention_key)
);

-- Study-Intervention (bridge table with study-specific attributes)
CREATE TABLE staging.study_interventions (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    intervention_key CHAR(16) NOT NULL REFERENCES staging.interventions(intervention_key),
    description TEXT, --study specific description
    is_primary_name BOOLEAN,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, intervention_key)
);

-- Other Intervention Names (dimension table for aliases)
CREATE TABLE staging.other_intervention_names (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name TEXT,
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Other Intervention Names 
CREATE TABLE staging.study_intervention_aliases (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    intervention_key CHAR(16) NOT NULL REFERENCES staging.other_intervention_names(intervention_key),
    description TEXT,
    is_primary_name BOOLEAN,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, intervention_key)
);

-- Primary Outcomes
CREATE TABLE staging.primary_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    measure TEXT,
    description TEXT,
    time_frame TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Secondary Outcomes
CREATE TABLE staging.secondary_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    measure TEXT,
    description TEXT,
    time_frame TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Other Outcomes
CREATE TABLE staging.other_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    measure TEXT,
    description TEXT,
    time_frame TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Central Contacts
CREATE TABLE staging.central_contacts (
    contact_key CHAR(16) PRIMARY KEY,
    name TEXT,
    role ContactRole,
    phone VARCHAR(30),
    phone_ext VARCHAR(20),
    email TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Central Contacts
CREATE TABLE staging.study_central_contacts (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    contact_key CHAR(16) NOT NULL REFERENCES staging.central_contacts(contact_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, contact_key)
);

-- Locations
CREATE TABLE staging.locations (
    location_key CHAR(16) PRIMARY KEY,
    facility TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Locations
CREATE TABLE staging.study_locations (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    location_key CHAR(16) NOT NULL REFERENCES staging.locations(location_key),
    status RecruitmentStatus,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, location_key)
);

CREATE TABLE staging.location_contacts (
    contact_key CHAR(16) PRIMARY KEY,
    name TEXT,
    role TEXT,
    phone TEXT,
    phone_ext TEXT,
    email TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
    );


CREATE TABLE staging.study_location_contacts (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    location_key CHAR(16) NOT NULL REFERENCES staging.study_locations(location_key),
    contact_key CHAR(16) NOT NULL REFERENCES staging.location_contacts(contact_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, location_key, contact_key)
);



CREATE INDEX idx_locations_country ON staging.locations(country);
CREATE INDEX idx_locations_country_city ON staging.locations(country, city);
CREATE INDEX idx_locations_geo ON staging.locations(lat, lon) WHERE lat IS NOT NULL AND lon IS NOT NULL;
CREATE INDEX idx_study_locations_status ON staging.study_locations(status);


-- Study References
CREATE TABLE staging.study_references (
    ref_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    pmid VARCHAR(20),
    type ReferenceType,
    citation TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Link References
CREATE TABLE staging.link_references (
    link_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    label TEXT,
    url TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- IPD References
CREATE TABLE staging.ipd_references (
    ipd_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    id VARCHAR(30),
    type TEXT,
    url TEXT,
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measures
CREATE TABLE staging.outcome_measures (
    outcome_measure_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    outcome_type OutcomeMeasureType,
    title TEXT,
    description TEXT,
    population_description TEXT,
    reporting_status ReportingStatus,
    anticipated_posting_date TEXT, -- partial date
    param_type MeasureParam,
    dispersion_type TEXT,
    unit_of_measure TEXT,
    calculate_pct BOOLEAN,
    time_frame TEXT,
    denom_units_selected TEXT,
    type_units_analysed TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measure Groups
CREATE TABLE staging.outcome_measure_groups (
    group_key CHAR(16) PRIMARY KEY,
    outcome_measure_key CHAR(16) NOT NULL REFERENCES staging.outcome_measures(outcome_measure_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    group_id VARCHAR(20),
    title TEXT,
    description TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measure Denominator Units 
CREATE TABLE staging.outcome_measure_denom_units (
    denom_unit_key CHAR(16) PRIMARY KEY,
    denom_unit TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measure Denominator Counts (sample sizes per group/unit)
CREATE TABLE staging.outcome_measure_denom_counts (
    denom_count_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES staging.outcome_measures(outcome_measure_key),
    denom_unit_key CHAR(16) NOT NULL REFERENCES staging.outcome_measure_denom_units(denom_unit_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.outcome_measure_groups(group_key),
    group_id VARCHAR(20),
    denom_value VARCHAR(20), --INT but API returns a string representation
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measure Groups Result (measurement values per group)
CREATE TABLE staging.outcome_measure_groups_result (
    group_key CHAR(16) NOT NULL REFERENCES staging.outcome_measure_groups(group_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES staging.outcome_measures(outcome_measure_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    group_id VARCHAR(20),
    value TEXT,
    lower_limit VARCHAR(20), --FLOAT but API returns a string representation
    upper_limit VARCHAR(20), --FLOAT but API returns a string representation
    spread VARCHAR(20), --FLOAT but API returns a string representation
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (group_key, outcome_measure_key, study_key)
);

-- Outcome Measure Analyses (statistical test results)
CREATE TABLE staging.outcome_measure_analyses (
    analysis_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES staging.outcome_measures(outcome_measure_key),
    param_type TEXT,
    param_value VARCHAR(20), --FLOAT but API returns a string representation
    dispersion_type AnalysisDispersionType,
    dispersion_value VARCHAR(20), --FLOAT but API returns a string representation
    statistical_method TEXT,
    statistical_comment TEXT,
    p_value VARCHAR(20), --FLOAT but API returns a string representation
    p_value_comment TEXT,
    ci_num_sides ConfidenceIntervalNumSides,
    ci_pct_value TEXT, --FLOAT but API returns a string representation
    ci_lower_limit TEXT, --FLOAT but API returns a string AND sometimes text
    ci_upper_limit TEXT, --FLOAT but API returns a string representation
    ci_lower_limit_cmt TEXT,
    ci_upper_limit_cmt TEXT,
    estimate_cmt TEXT,
    tested_non_inferiority BOOLEAN,
    non_inferiority_type NonInferiorityType,
    non_inferiority_comment TEXT,
    other_analysis_desc TEXT,
    group_desc TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measure Comparison Groups
CREATE TABLE staging.outcome_measure_comparison_groups (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES staging.outcome_measures(outcome_measure_key),
    analysis_key CHAR(16) NOT NULL REFERENCES staging.outcome_measure_analyses(analysis_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.outcome_measure_groups(group_key),
    group_id VARCHAR(20),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, outcome_measure_key, analysis_key, group_key)
);

-- Flow Groups
CREATE TABLE staging.flow_groups (
    group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    id VARCHAR(20),
    title TEXT,
    description TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Flow Periods
CREATE TABLE staging.flow_periods (
    period_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    title VARCHAR(40),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Flow Period Milestones
CREATE TABLE staging.flow_period_milestones (
    milestone_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES staging.flow_periods(period_key),
    type TEXT,
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Flow Period Milestone Achievements
CREATE TABLE staging.flow_period_milestone_achievements (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES staging.flow_periods(period_key),
    milestone_key CHAR(16) NOT NULL REFERENCES staging.flow_period_milestones(milestone_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.flow_groups(group_key),
    group_id VARCHAR(20),
    comment TEXT,
    num_subjects VARCHAR(20),
    num_units VARCHAR(20),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, period_key, milestone_key, group_key)
);

-- Withdrawal types 
CREATE TABLE staging.withdrawal_types (
    withdrawal_type_key CHAR(16) PRIMARY KEY,
    type TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);


-- Flow Period Withdrawals
CREATE TABLE staging.flow_period_withdrawals (
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES staging.flow_periods(period_key),
    withdrawal_type_key CHAR(16) NOT NULL REFERENCES staging.withdrawal_types(withdrawal_type_key),
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, period_key, withdrawal_type_key)
);

-- Flow Period Withdrawal Reasons
CREATE TABLE staging.flow_period_withdrawal_reasons (
    reason_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES staging.flow_periods(period_key),
    withdrawal_type_key CHAR(16) NOT NULL REFERENCES staging.withdrawal_types(withdrawal_type_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.flow_groups(group_key),
    group_id VARCHAR(20),
    reason TEXT,
    num_subjects VARCHAR(20), --INT but API returns a string representation
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Adverse Events
CREATE TABLE staging.adverse_events (
    adverse_event_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    description TEXT,
    frequency_threshold VARCHAR(20), --INT/FLOAT but API returns a string representation
    time_frame TEXT,
    mortality_cmt TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Event Groups
CREATE TABLE staging.event_groups (
    event_group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    adverse_event_key CHAR(16) NOT NULL REFERENCES staging.adverse_events(adverse_event_key),
    group_id VARCHAR(20),
    title TEXT,
    description TEXT,
    num_deaths FLOAT,
    num_deaths_at_risk FLOAT,
    num_serious FLOAT,
    num_serious_at_risk FLOAT,
    num_other FLOAT,
    num_other_at_risk FLOAT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Serious Events
CREATE TABLE staging.serious_events (
    serious_event_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES staging.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    term TEXT,
    organ_system TEXT,
    source_vocab TEXT,
    assessment_type EventAssessment,
    notes TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Serious Event Stats
CREATE TABLE staging.serious_event_stats (
    event_stat_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES staging.adverse_events(adverse_event_key),
    serious_event_key CHAR(16) NOT NULL REFERENCES staging.serious_events(serious_event_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.event_groups(event_group_key),
    group_id VARCHAR(20),
    num_events FLOAT,
    num_affected FLOAT,
    num_at_risk FLOAT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Other Events
CREATE TABLE staging.other_events (
    other_event_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES staging.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    term TEXT,
    organ_system TEXT,
    source_vocab VARCHAR(20),
    assessment_type EventAssessment,
    notes TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Other Event Stats
CREATE TABLE staging.other_event_stats (
    event_stat_key CHAR(16) PRIMARY KEY,
    other_event_key CHAR(16) NOT NULL REFERENCES staging.other_events(other_event_key),
    adverse_event_key CHAR(16) NOT NULL REFERENCES staging.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    group_key CHAR(16) NOT NULL REFERENCES staging.event_groups(event_group_key),
    group_id VARCHAR(20),
    num_events FLOAT,
    num_affected FLOAT,
    num_at_risk FLOAT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- FDAAA 801 Violations
CREATE TABLE staging.violations (
    violation_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    violation_type ViolationEventType,
    issued_date DATE,
    description TEXT,
    creation_date DATE,
    release_date DATE,
    posted_date DATE,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Conditions MeSH 
CREATE TABLE staging.conditions_mesh (
    mesh_key CHAR(16) PRIMARY KEY,
    mesh_id VARCHAR(20),
    mesh_term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Conditions MESH 
CREATE TABLE staging.study_conditions_mesh (
    mesh_key CHAR(16) NOT NULL REFERENCES staging.conditions_mesh(mesh_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (mesh_key, study_key)
);

-- Conditions MeSH Ancestors dimension table (parent terms in MeSH tree)
CREATE TABLE staging.conditions_mesh_ancestors (
    ancestor_key CHAR(16) PRIMARY KEY,
    ancestor_id VARCHAR(20),
    ancestor_term TEXT,
    term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Conditions MeSH Ancestors 
CREATE TABLE staging.study_conditions_mesh_ancestors (
    ancestor_key CHAR(16) NOT NULL REFERENCES staging.conditions_mesh_ancestors(ancestor_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (ancestor_key, study_key)
);

-- Conditions Browse Leaves 
CREATE TABLE staging.conditions_browse_leaves (
    leaf_key CHAR(16) PRIMARY KEY,
    leaf_id VARCHAR(20),
    name TEXT,
    as_found TEXT,
    relevance BrowseLeafRelevance,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Conditions Browse 
CREATE TABLE staging.study_conditions_browse_leaves (
    leaf_key CHAR(16) NOT NULL REFERENCES staging.conditions_browse_leaves(leaf_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (leaf_key, study_key)
);

-- Conditions Browse Branches 
CREATE TABLE staging.conditions_browse_branches (
    branch_key CHAR(16) PRIMARY KEY,
    abbrev TEXT,
    name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Conditions Browse Branches 
CREATE TABLE staging.study_conditions_browse_branches (
    branch_key CHAR(16) NOT NULL REFERENCES staging.conditions_browse_branches(branch_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (branch_key, study_key)
);

-- Interventions MeSH 
CREATE TABLE staging.interventions_mesh (
    mesh_key CHAR(16) PRIMARY KEY,
    mesh_id VARCHAR(20),
    mesh_term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Interventions MESH 
CREATE TABLE staging.study_interventions_mesh (
    mesh_key CHAR(16) NOT NULL REFERENCES staging.interventions_mesh(mesh_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (mesh_key, study_key)
);

-- Interventions MeSH Ancestors dimension table (parent terms in MeSH tree)
CREATE TABLE staging.interventions_mesh_ancestors (
    ancestor_key CHAR(16) PRIMARY KEY,
    ancestor_id VARCHAR(20),
    ancestor_term TEXT,
    term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Interventions MeSH Ancestors 
CREATE TABLE staging.study_interventions_mesh_ancestors (
    ancestor_key CHAR(16) NOT NULL REFERENCES staging.interventions_mesh_ancestors(ancestor_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (ancestor_key, study_key)
);

-- Interventions Browse Leaves 
CREATE TABLE staging.interventions_browse_leaves (
    leaf_key CHAR(16) PRIMARY KEY,
    leaf_id VARCHAR(20),
    name TEXT,
    as_found TEXT,
    relevance BrowseLeafRelevance,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Interventions Browse 
CREATE TABLE staging.study_interventions_browse_leaves (
    leaf_key CHAR(16) NOT NULL REFERENCES staging.interventions_browse_leaves(leaf_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (leaf_key, study_key)
);

-- Interventions Browse Branches 
CREATE TABLE staging.interventions_browse_branches (
    branch_key CHAR(16) PRIMARY KEY,
    abbrev TEXT,
    name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Interventions Browse Branches 
CREATE TABLE staging.study_interventions_browse_branches (
    branch_key CHAR(16) NOT NULL REFERENCES staging.interventions_browse_branches(branch_key),
    study_key CHAR(16) NOT NULL REFERENCES staging.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (branch_key, study_key)
);

