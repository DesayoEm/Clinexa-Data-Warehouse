CREATE SCHEMA staging;
CREATE SCHEMA patient_matching;
CREATE SCHEMA landscape;
CREATE SCHEMA r_and_d;

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

CREATE TYPE EnrollmentType AS ENUM(
    'ACTUAL',
    'ESTIMATED'
);

CREATE TYPE DesignAllocation AS ENUM(
    'RANDOMIZED',
    'NON_RANDOMIZED',
    'N/A'
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

CREATE TYPE SecondaryIdType AS ENUM(
    'NIH',
    'FDA',
    'VA',
    'CDC',
    'AHRQ',
    'SAMHSA',
    'OTHER_GRANT',
    'EUDRACT_NUMBER',
    'CTIS',
    'OTHER'
);

CREATE TYPE AgencyClass AS ENUM(
    'NIH',
    'FED',
    'OTHER_GOV',
    'INDIV',
    'INDUSTRY',
    'NETWORK',
    'AMBIG',
    'OTHER',
    'UNKNOWN'
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

CREATE TYPE MeasureDispersionType AS ENUM(
    'NA',
    'STANDARD_DEVIATION',
    'STANDARD_ERROR',
    'INTER_QUARTILE_RANGE',
    'FULL_RANGE',
    'CONFIDENCE_80',
    'CONFIDENCE_90',
    'CONFIDENCE_95',
    'CONFIDENCE_975',
    'CONFIDENCE_99',
    'CONFIDENCE_OTHER',
    'GEOMETRIC_COEFFICIENT'
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

--- fixed character lengths are created using the registry docs as as a guide



-- STUDIES
CREATE TABLE IF NOT EXISTS staging.studies(
    -- Primary Key
    study_key CHAR(16) PRIMARY KEY,

    -- Identifiers
    nct_id VARCHAR(15) NOT NULL UNIQUE,
    org_study_id VARCHAR(30),

    -- Study Titles & Descriptions
    brief_title VARCHAR(300),
    official_title VARCHAR(600),
    acronym VARCHAR(14),
    brief_summary VARCHAR(5000),
    detailed_desc VARCHAR(32000),

    -- Administrative
    responsible_party_type ResponsiblePartyType,

    -- Study Classification
    study_type StudyType,
    patient_registry BOOLEAN,

    -- Enrollment
    enrollment_type EnrollmentType,
    enrollment_count FLOAT, -- Ideally should be an int but API returns a float

    -- Design - Interventional
    design_allocation DesignAllocation,
    design_intervention_model InterventionalAssignment,
    design_intervention_model_desc VARCHAR(1000),
    design_primary_purpose PrimaryPurpose,
    design_masking DesignMasking,

    -- Design - Observational
    design_observational_model ObservationalModel,
    design_time_perspective DesignTimePerspective,

    -- Biospecimens
    biospec_retention BioSpecRetention,
    biospec_desc VARCHAR(1000),

    -- Eligibility
    eligibility_criteria TEXT,
    healthy_volunteers BOOLEAN,
    sex BioSex,
    min_age VARCHAR(20),
    max_age VARCHAR(20),
    population_desc VARCHAR(1000),
    sampling_method SamplingMethod,

    -- Status & Dates
    overall_status Status,
    last_known_status Status,
    status_verified_date DATE,
    start_date DATE,
    start_date_type DateType,
    first_submit_date DATE,
    last_update_submit_date DATE,
    completion_date DATE,
    completion_date_type DateType,
    why_stopped VARCHAR(250),

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
    ipd_desc VARCHAR(1000),
    ipd_time_frame VARCHAR(1000),
    ipd_access_criteria VARCHAR(1000),
    ipd_url VARCHAR(3999),

    -- Point of Contact
    poc_title VARCHAR(20),
    poc_organization VARCHAR(200),
    poc_email VARCHAR(200),
    poc_phone VARCHAR(30),
    poc_phone_ext VARCHAR(20),

    -- Participant Flow
    flow_pre_assignment_details VARCHAR(500),
    flow_recruitment_details VARCHAR(500),
    flow_type_units_analysed VARCHAR(40),

    -- Agreements
    certain_agreement_pi_sponsor_employee BOOLEAN,
    certain_agreement_restrictive BOOLEAN,
    certain_agreement_restriction_type AgreementRestrictionType,
    certain_agreement_other_details VARCHAR(500),

    -- Results & Tracking
    sub_tracking_estimated_results_date DATE,
    has_results BOOLEAN,
    limitations_desc VARCHAR(500),

    -- Metadata
    version_holder DATE,
    last_updated DATE,

    -- ETL Audit Cols
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STUDIES INDEXES

CREATE INDEX idx_studies_overall_status ON staging.studies(overall_status);
CREATE INDEX idx_studies_study_type ON staging.studies(study_type);
CREATE INDEX idx_studies_last_updated ON staging.studies(last_updated);
CREATE INDEX idx_studies_loaded_at ON staging.studies(loaded_at);
CREATE INDEX idx_studies_execution_date ON staging.studies(execution_date);


-- Secondary IDs
CREATE TABLE staging.secondary_ids (
    secondary_id_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    id VARCHAR(30),
    type SecondaryIdType,
    domain VARCHAR(120),
    link TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NCT Aliases
CREATE TABLE staging.nct_aliases (
    id_alias_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    id_alias VARCHAR(15) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- Lead Sponsor
CREATE TABLE staging.sponsors (
    sponsor_key CHAR(16) PRIMARY KEY,
    name TEXT,
    sponsor_class AgencyClass,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Sponsor
CREATE TABLE staging.study_sponsors (
    study_key CHAR(16) NOT NULL,
    sponsor_key CHAR(16) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, sponsor_key)
);

-- Collaborator
CREATE TABLE staging.collaborators (
    collaborator_key CHAR(16) PRIMARY KEY,
    name VARCHAR(160),
    collaborator_class AgencyClass,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Collaborator
CREATE TABLE staging.study_collaborators (
    study_key CHAR(16) NOT NULL,
    collaborator_key CHAR(16) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, collaborator_key)
);

-- ConditionS
CREATE TABLE staging.conditions (
    condition_key CHAR(16) PRIMARY KEY,
    condition_name VARCHAR(200),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Condition
CREATE TABLE staging.study_conditions (
    study_key CHAR(16) NOT NULL,
    condition_key CHAR(16) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, condition_key)
);

-- Keywords
CREATE TABLE staging.keywords (
    keyword_key CHAR(16) PRIMARY KEY,
    keyword_name VARCHAR(200),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Keyword
CREATE TABLE staging.study_keywords (
    study_key CHAR(16) NOT NULL,
    keyword_key CHAR(16) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, keyword_key)
);

-- Arm Groups
CREATE TABLE staging.arm_groups (
    arm_group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    arm_label VARCHAR(100),
    arm_description VARCHAR(999),
    arm_type ArmGroupType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Arm-Intervention
CREATE TABLE staging.arm_interventions (
    study_key CHAR(16) NOT NULL,
    arm_group_key CHAR(16) NOT NULL,
    arm_intervention_key CHAR(16) NOT NULL,
    arm_intervention_name VARCHAR(200),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, arm_group_key, arm_intervention_key)
);

-- Interventions
CREATE TABLE staging.interventions (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name VARCHAR(200),
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Intervention
CREATE TABLE staging.study_interventions (
    study_key CHAR(16) NOT NULL,
    intervention_key CHAR(16) NOT NULL,
    description VARCHAR(1000), --study specific description
    is_primary_name BOOLEAN,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, intervention_key)
);

-- Other Intervention Names
CREATE TABLE staging.other_intervention_names (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name VARCHAR(200),
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study-Other Intervention Names
CREATE TABLE staging.study_intervention_aliases (
    study_key CHAR(16) NOT NULL,
    intervention_key CHAR(16) NOT NULL,
    description VARCHAR(1000),
    is_primary_name BOOLEAN,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, intervention_key)
);

-- Primary Outcomes
CREATE TABLE staging.primary_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    measure VARCHAR(254),
    description VARCHAR(999),
    time_frame VARCHAR(254),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Secondary Outcomes
CREATE TABLE staging.secondary_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    measure VARCHAR(254),
    description VARCHAR(999),
    time_frame VARCHAR(254),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Other Outcomes
CREATE TABLE staging.other_outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    measure VARCHAR(254),
    description VARCHAR(999),
    time_frame VARCHAR(254),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Study References
CREATE TABLE staging.study_references (
    ref_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    pmid VARCHAR(20),
    type ReferenceType,
    citation VARCHAR(2000),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Link References
CREATE TABLE staging.link_references (
    link_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    label VARCHAR(254),
    url VARCHAR(3999),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- IPD References
CREATE TABLE staging.ipd_references (
    ipd_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    id VARCHAR(30),
    type VARCHAR(100),
    url VARCHAR(3999),
    comment VARCHAR(1000),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measures
CREATE TABLE staging.outcome_measures (
    outcome_measure_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    outcome_type OutcomeMeasureType,
    title VARCHAR(255),
    description VARCHAR(999),
    population_description VARCHAR(500),
    reporting_status ReportingStatus,
    anticipated_posting_date DATE,
    param_type MeasureParam,
    dispersion_type MeasureDispersionType,
    unit_of_measure VARCHAR(40),
    calculate_pct BOOLEAN,
    time_frame VARCHAR(255),
    denom_units_selected TEXT,
    type_units_analysed VARCHAR(40),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measure Groups
CREATE TABLE staging.outcome_measure_groups (
    group_key CHAR(16) PRIMARY KEY,
    outcome_measure_key CHAR(16) NOT NULL,
    study_key CHAR(16) NOT NULL,
    group_id VARCHAR(20),
    title VARCHAR(100),
    description TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measure Denominator Units
CREATE TABLE staging.outcome_measure_denom_units (
    denom_unit_key CHAR(16) PRIMARY KEY,
    denom_unit VARCHAR(100),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measure Denominator Counts  (sample sizes per group/unit)
CREATE TABLE staging.outcome_measure_denom_counts (
    denom_count_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    outcome_measure_key CHAR(16) NOT NULL,
    denom_unit_key CHAR(16) NOT NULL,
    group_key CHAR(16) NOT NULL,
    group_id VARCHAR(20),
    denom_value VARCHAR(20), --INT but API returns a string representation
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measure Groups Result (measurement values per group)
CREATE TABLE staging.outcome_measure_groups_result (
    group_key CHAR(16) NOT NULL,
    outcome_measure_key CHAR(16) NOT NULL,
    study_key CHAR(16) NOT NULL,
    group_id VARCHAR(20),
    value VARCHAR(500),
    lower_limit VARCHAR(20), --FLOAT but API returns a string representation
    upper_limit VARCHAR(20), --FLOAT but API returns a string representation
    spread VARCHAR(20), --FLOAT but API returns a string representation
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_key, outcome_measure_key, study_key)
);

-- Outcome Measure Analyses (statistical test results)
CREATE TABLE staging.outcome_measure_analyses (
    analysis_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL,
    outcome_measure_key CHAR(16) NOT NULL,
    param_type VARCHAR(50),
    param_value VARCHAR(20), --FLOAT but API returns a string representation
    dispersion_type AnalysisDispersionType,
    dispersion_value VARCHAR(20), --FLOAT but API returns a string representation
    statistical_method VARCHAR(50),
    statistical_comment VARCHAR(10),
    p_value VARCHAR(20), --FLOAT but API returns a string representation
    p_value_comment VARCHAR(250),
    ci_num_sides ConfidenceIntervalNumSides,
    ci_pct_value VARCHAR(20), --FLOAT but API returns a string representation
    ci_lower_limit VARCHAR(20), --FLOAT but API returns a string representation
    ci_upper_limit VARCHAR(20), --FLOAT but API returns a string representation
    ci_lower_limit_cmt VARCHAR(250),
    ci_upper_limit_cmt VARCHAR(250),
    estimate_cmt VARCHAR(250),
    tested_non_inferiority BOOLEAN,
    non_inferiority_type NonInferiorityType,
    other_analysis_desc TEXT,
    group_desc VARCHAR(500),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcome Measure Comparison Groups
CREATE TABLE staging.outcome_measure_comparison_groups (
    study_key CHAR(16) NOT NULL,
    outcome_measure_key CHAR(16) NOT NULL,
    analysis_key CHAR(16) NOT NULL,
    group_key CHAR(16) NOT NULL,
    group_id VARCHAR(10),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (study_key, outcome_measure_key, analysis_key, group_key)
);