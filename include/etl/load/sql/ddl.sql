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
--- fixed character lengths are created using the registry docs as as a guide

-- STAGING TABLES

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
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id UUID,
    source_file VARCHAR(255)
);

-- INDEXES

CREATE INDEX idx_studies_overall_status ON staging.studies(overall_status);
CREATE INDEX idx_studies_study_type ON staging.studies(study_type);
CREATE INDEX idx_studies_last_updated ON staging.studies(last_updated);
CREATE INDEX idx_studies_loaded_at ON staging.studies(loaded_at);

