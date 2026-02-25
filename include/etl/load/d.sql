CREATE TABLE IF NOT EXISTS outcome.studies(
    study_key CHAR(16) PRIMARY KEY,
    nct_id VARCHAR(15) NOT NULL UNIQUE,
    brief_title VARCHAR(300),
    official_title VARCHAR(600),
    detailed_desc TEXT,
    responsible_party_type ResponsiblePartyType,
    study_type StudyType,
    patient_registry BOOLEAN,
    enrollment_type TEXT,
    enrollment_count FLOAT,
    design_intervention_model_desc TEXT,
    biospec_desc TEXT,
    eligibility_criteria TEXT,
    healthy_volunteers BOOLEAN,
    sex BioSex,
    min_age_value INTEGER,
    min_age_metric VARCHAR(20),
    max_age_value INTEGER,
    max_age_metric VARCHAR(20),
    population_desc TEXT,

    -- Design - Interventional
    design_allocation DesignAllocation,
    design_intervention_model InterventionalAssignment,
    design_primary_purpose PrimaryPurpose,
    design_masking DesignMasking,
    design_observational_model ObservationalModel,
    design_time_perspective DesignTimePerspective,
    biospec_retention BioSpecRetention,
    sampling_method SamplingMethod,

    -- Status & Dates
    overall_status Status,
    last_known_status Status,
    status_verified_date TEXT, --PARTIAL DATE
    start_date TEXT, --partial date
    start_date_type DateType,
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
    ipd_sharing IpdSharing,
    ipd_desc TEXT,
    ipd_time_frame TEXT,
    ipd_access_criteria TEXT,
    ipd_url TEXT,
    flow_pre_assignment_details TEXT,
    flow_recruitment_details TEXT,
    flow_type_units_analysed TEXT,
    -- Results & Tracking
    sub_tracking_estimated_results_date TEXT, --Partial date
    has_results BOOLEAN,
    limitations_desc TEXT,
    version_holder DATE,
    last_updated DATE,

    -- Audit
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);


-- Sponsor
CREATE TABLE outcome.sponsors (
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
CREATE TABLE outcome.study_sponsors (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    sponsor_key CHAR(16) NOT NULL REFERENCES outcome.sponsors(sponsor_key),
    is_lead
     BOOLEAN,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, sponsor_key)
);

-- Conditions
CREATE TABLE outcome.conditions (
    condition_key CHAR(16) PRIMARY KEY,
    mesh_id VARCHAR(20),
    condition TEXT,
    abbrev TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Condition 
CREATE TABLE outcome.study_conditions (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    condition_key CHAR(16) NOT NULL REFERENCES outcome.conditions(condition_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, condition_key)
);

-- Arm Groups
CREATE TABLE outcome.arm_groups (
    arm_group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    arm_label VARCHAR(100),
    arm_description TEXT,
    arm_type ArmGroupType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Arm-Intervention
CREATE TABLE outcome.arm_interventions (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    arm_group_key CHAR(16) NOT NULL REFERENCES outcome.arm_groups(arm_group_key),
    arm_intervention_key CHAR(16) NOT NULL REFERENCES outcome.interventions(intervention_key),
    arm_intervention_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, arm_group_key, arm_intervention_key)
);

-- Interventions 
CREATE TABLE outcome.interventions (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name TEXT,
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Study-Intervention
CREATE TABLE outcome.study_interventions (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    intervention_key CHAR(16) NOT NULL REFERENCES outcome.interventions(intervention_key),
    description TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, intervention_key)
);

-- Outcomes
CREATE TABLE outcome.outcomes (
    outcome_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    measure TEXT,
    description TEXT,
    time_frame TEXT,
    outcome_type OutcomeMeasureType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- Outcome Measures
CREATE TABLE outcome.outcome_measures (
    outcome_measure_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.outcome_measure_groups (
    group_key CHAR(16) PRIMARY KEY,
    outcome_measure_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measures(outcome_measure_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.outcome_measure_denom_units (
    denom_unit_key CHAR(16) PRIMARY KEY,
    denom_unit TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
);

-- Outcome Measure Denominator Counts (sample sizes per group/unit)
CREATE TABLE outcome.outcome_measure_denom_counts (
    denom_count_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measures(outcome_measure_key),
    denom_unit_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measure_denom_units(denom_unit_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measure_groups(group_key),
    group_id VARCHAR(20),
    denom_value VARCHAR(20), --INT but API returns a string representation
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
);

-- Outcome Measure Groups Result (measurement values per group)
CREATE TABLE outcome.outcome_measure_groups_result (
    group_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measure_groups(group_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measures(outcome_measure_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.outcome_measure_analyses (
    analysis_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measures(outcome_measure_key),
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
CREATE TABLE outcome.outcome_measure_comparison_groups (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    outcome_measure_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measures(outcome_measure_key),
    analysis_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measure_analyses(analysis_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.outcome_measure_groups(group_key),
    group_id VARCHAR(20),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, outcome_measure_key, analysis_key, group_key)
);

-- Flow Groups
CREATE TABLE outcome.flow_groups (
    group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.flow_periods (
    period_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    title VARCHAR(40),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
);

-- Flow Period Milestones
CREATE TABLE outcome.flow_period_milestones (
    milestone_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES outcome.flow_periods(period_key),
    type TEXT,
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
);

-- Flow Period Milestone Achievements
CREATE TABLE outcome.flow_period_milestone_achievements (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES outcome.flow_periods(period_key),
    milestone_key CHAR(16) NOT NULL REFERENCES outcome.flow_period_milestones(milestone_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.flow_groups(group_key),
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
CREATE TABLE outcome.withdrawal_types (
    withdrawal_type_key CHAR(16) PRIMARY KEY,
    type TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE
);


-- Flow Period Withdrawals
CREATE TABLE outcome.flow_period_withdrawals (
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES outcome.flow_periods(period_key),
    withdrawal_type_key CHAR(16) NOT NULL REFERENCES outcome.withdrawal_types(withdrawal_type_key),
    comment TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    PRIMARY KEY (study_key, period_key, withdrawal_type_key)
);

-- Flow Period Withdrawal Reasons
CREATE TABLE outcome.flow_period_withdrawal_reasons (
    reason_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    period_key CHAR(16) NOT NULL REFERENCES outcome.flow_periods(period_key),
    withdrawal_type_key CHAR(16) NOT NULL REFERENCES outcome.withdrawal_types(withdrawal_type_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.flow_groups(group_key),
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
CREATE TABLE outcome.adverse_events (
    adverse_event_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.event_groups (
    event_group_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    adverse_event_key CHAR(16) NOT NULL REFERENCES outcome.adverse_events(adverse_event_key),
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
CREATE TABLE outcome.serious_events (
    serious_event_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES outcome.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.serious_event_stats (
    event_stat_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES outcome.adverse_events(adverse_event_key),
    serious_event_key CHAR(16) NOT NULL REFERENCES outcome.serious_events(serious_event_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.event_groups(event_group_key),
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
CREATE TABLE outcome.other_events (
    other_event_key CHAR(16) PRIMARY KEY,
    adverse_event_key CHAR(16) NOT NULL REFERENCES outcome.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.other_event_stats (
    event_stat_key CHAR(16) PRIMARY KEY,
    other_event_key CHAR(16) NOT NULL REFERENCES outcome.other_events(other_event_key),
    adverse_event_key CHAR(16) NOT NULL REFERENCES outcome.adverse_events(adverse_event_key),
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    group_key CHAR(16) NOT NULL REFERENCES outcome.event_groups(event_group_key),
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

-- Study References
CREATE TABLE outcome.study_references (
    ref_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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
CREATE TABLE outcome.link_references (
    link_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
    label TEXT,
    url TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE 
);

-- IPD References
CREATE TABLE outcome.ipd_references (
    ipd_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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


-- FDAAA 801 Violations
CREATE TABLE outcome.violations (
    violation_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES outcome.studies(study_key),
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

