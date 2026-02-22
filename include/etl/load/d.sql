

--- ENROLLMENT
--------------

CREATE TABLE IF NOT EXISTS enrollment.studies(
    study_key CHAR(16) PRIMARY KEY,
    nct_id VARCHAR(15) NOT NULL UNIQUE,
    org_study_id VARCHAR(30),
    brief_title VARCHAR(300),
    official_title VARCHAR(600),
    acronym VARCHAR(14),
    brief_summary TEXT,
    detailed_desc TEXT,
    responsible_party_type ResponsiblePartyType,
    study_type StudyType,
    patient_registry BOOLEAN,
    enrollment_type TEXT,
    enrollment_count INTEGER
    design_allocation DesignAllocation,
    design_intervention_model InterventionalAssignment,
    design_intervention_model_desc TEXT,
    design_primary_purpose PrimaryPurpose,
    design_masking DesignMasking,
    design_observational_model ObservationalModel,
    design_time_perspective DesignTimePerspective,
    biospec_retention BioSpecRetention,
    biospec_desc TEXT,
    eligibility_criteria TEXT,
    healthy_volunteers BOOLEAN,
    sex BioSex,
    min_age VARCHAR(20),
    max_age VARCHAR(20),
    age_group VARCHAR (20),
    population_desc TEXT,
    overall_status Status,
    is_active BOOLEAN
    last_known_status Status,
    status_verified_date TEXT,
    status_verified_date_parsed DATE,
    start_date TEXT
    start_date_parsed DATE
    start_date_type DateType,
    completion_date TEXT,
    completion_date_parsed DATE,
    completion_date_type DateType,
    has_expanded_access BOOLEAN,
    expanded_access_nct VARCHAR(15),
    is_fda_regulated_drug BOOLEAN,
    is_fda_regulated_device BOOLEAN,
    is_unapproved_device BOOLEAN,
    is_us_export BOOLEAN,
    poc_title TEXT,
    poc_organization TEXT,
    poc_email TEXT,
    poc_phone TEXT,
    poc_phone_ext TEXT,
    last_updated DATE,
    -- Audit
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE
);


-- Secondary IDs
CREATE TABLE enrollment.secondary_ids (
    secondary_id_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    id VARCHAR(30),
    type TEXT,
    domain TEXT,
    link TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- NCT Aliases
CREATE TABLE enrollment.nct_aliases (
    id_alias_key CHAR(16) PRIMARY KEY,
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    id_alias VARCHAR(15) NOT NULL,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Lead Sponsor
CREATE TABLE enrollment.sponsors (
    sponsor_key CHAR(16) PRIMARY KEY,
    name TEXT,
    sponsor_class TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Sponsor
CREATE TABLE enrollment.study_sponsors (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    sponsor_key CHAR(16) NOT NULL REFERENCES enrollment.sponsors(sponsor_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    PRIMARY KEY (study_key, sponsor_key),
    dbt_created_on DATE
);

-- Study-Collaborator
CREATE TABLE enrollment.study_collaborators (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    collaborator_key CHAR(16) NOT NULL REFERENCES enrollment.sponsors(sponsor_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    PRIMARY KEY (study_key, collaborator_key)
);

-- Conditions
CREATE TABLE enrollment.conditions (
    condition_key CHAR(16) PRIMARY KEY,
    condition_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    dbt_created_on DATE,
    last_seen_on DATE
);

-- Study-Condition
CREATE TABLE enrollment.study_conditions (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    condition_key CHAR(16) NOT NULL REFERENCES enrollment.conditions(condition_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (study_key, condition_key)
);

-- Keywords
CREATE TABLE enrollment.keywords (
    keyword_key CHAR(16) PRIMARY KEY,
    keyword_name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Keyword
CREATE TABLE enrollment.study_keywords (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    keyword_key CHAR(16) NOT NULL REFERENCES enrollment.keywords(keyword_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (study_key, keyword_key)
);


-- Interventions
CREATE TABLE enrollment.interventions (
    intervention_key CHAR(16) PRIMARY KEY,
    intervention_name TEXT,
    intervention_type InterventionType,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE
);


-- Study-Intervention (bridge table with study-specific attributes)
CREATE TABLE enrollment.study_interventions (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    intervention_key CHAR(16) NOT NULL REFERENCES enrollment.interventions(intervention_key),
    description TEXT, --study specific description
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (study_key, intervention_key)
);


-- Study-Other Intervention Names
CREATE TABLE enrollment.study_intervention_aliases (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    intervention_key CHAR(16) NOT NULL REFERENCES enrollment.interventions(intervention_key),
    description TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (study_key, intervention_key)
);

-- Central Contacts
CREATE TABLE enrollment.contacts (
    contact_key CHAR(16) PRIMARY KEY,
    name TEXT,
    role ContactRole,
    phone VARCHAR(30),
    phone_ext VARCHAR(20),
    email TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Central Contacts
CREATE TABLE enrollment.study_central_contacts (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    contact_key CHAR(16) NOT NULL REFERENCES enrollment.contacts(contact_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE,
    PRIMARY KEY (study_key, contact_key)
);

CREATE TABLE enrollment.study_location_contacts (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    location_key CHAR(16) NOT NULL REFERENCES enrollment.locations(location_key),
    contact_key CHAR(16) NOT NULL REFERENCES enrollment.contacts(contact_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE,
    PRIMARY KEY (study_key, location_key, contact_key)
);

-- Locations
CREATE TABLE enrollment.locations (
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
    dbt_created_on DATE
);

-- Study-Locations
CREATE TABLE enrollment.study_locations (
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    location_key CHAR(16) NOT NULL REFERENCES enrollment.locations(location_key),
    status RecruitmentStatus,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (study_key, location_key)
);




-- Conditions MeSH
CREATE TABLE enrollment.condition_meshes (
    mesh_key CHAR(16) PRIMARY KEY,
    mesh_id VARCHAR(20),
    mesh_term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Conditions MESH
CREATE TABLE enrollment.study_condition_meshes (
    mesh_key CHAR(16) NOT NULL REFERENCES enrollment.condition_meshes(mesh_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE,
    PRIMARY KEY (mesh_key, study_key)
);

-- Conditions MeSH Ancestors dimension table (parent terms in MeSH tree)
CREATE TABLE enrollment.condition_mesh_ancestors (
    ancestor_key CHAR(16) PRIMARY KEY,
    ancestor_id VARCHAR(20),
    ancestor_term TEXT,
    term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Conditions MeSH Ancestors
CREATE TABLE enrollment.study_condition_mesh_ancestors (
    ancestor_key CHAR(16) NOT NULL REFERENCES enrollment.condition_mesh_ancestors(ancestor_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (ancestor_key, study_key)
);

-- Conditions Browse Leaves
CREATE TABLE enrollment.condition_browse_leaves (
    leaf_key CHAR(16) PRIMARY KEY,
    leaf_id VARCHAR(20),
    name TEXT,
    as_found TEXT,
    relevance BrowseLeafRelevance,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Conditions Browse
CREATE TABLE enrollment.study_condition_browse_leaves (
    leaf_key CHAR(16) NOT NULL REFERENCES enrollment.condition_browse_leaves(leaf_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (leaf_key, study_key)
);

-- Conditions Browse Branches
CREATE TABLE enrollment.condition_browse_branches (
    branch_key CHAR(16) PRIMARY KEY,
    abbrev TEXT,
    name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Conditions Browse Branches
CREATE TABLE enrollment.study_condition_browse_branches (
    branch_key CHAR(16) NOT NULL REFERENCES enrollment.condition_browse_branches(branch_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (branch_key, study_key)
);

-- Interventions MeSH
CREATE TABLE enrollment.intervention_meshes (
    mesh_key CHAR(16) PRIMARY KEY,
    mesh_id VARCHAR(20),
    mesh_term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Interventions MESH
CREATE TABLE enrollment.study_intervention_meshes (
    mesh_key CHAR(16) NOT NULL REFERENCES enrollment.intervention_meshes(mesh_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (mesh_key, study_key)
);

-- Interventions MeSH Ancestors
CREATE TABLE enrollment.intervention_mesh_ancestors (
    ancestor_key CHAR(16) PRIMARY KEY,
    ancestor_id VARCHAR(20),
    ancestor_term TEXT,
    term TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Interventions MeSH Ancestors
CREATE TABLE enrollment.study_intervention_mesh_ancestors (
    ancestor_key CHAR(16) NOT NULL REFERENCES enrollment.intervention_mesh_ancestors(ancestor_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (ancestor_key, study_key)
);

-- Interventions Browse Leaves
CREATE TABLE enrollment.intervention_browse_leaves (
    leaf_key CHAR(16) PRIMARY KEY,
    leaf_id VARCHAR(20),
    name TEXT,
    as_found TEXT,
    relevance BrowseLeafRelevance,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Interventions Browse
CREATE TABLE enrollment.study_intervention_browse_leaves (
    leaf_key CHAR(16) NOT NULL REFERENCES enrollment.intervention_browse_leaves(leaf_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (leaf_key, study_key)
);

-- Interventions Browse Branches
CREATE TABLE enrollment.intervention_browse_branches (
    branch_key CHAR(16) PRIMARY KEY,
    abbrev TEXT,
    name TEXT,
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    dbt_created_on DATE
);

-- Study-Interventions Browse Branches
CREATE TABLE enrollment.study_intervention_browse_branches (
    branch_key CHAR(16) NOT NULL REFERENCES enrollment.intervention_browse_branches(branch_key),
    study_key CHAR(16) NOT NULL REFERENCES enrollment.studies(study_key),
    dag_execution_date DATE,
    dag_id VARCHAR(100),
    dag_run_id VARCHAR(100),
    first_loaded_on DATE,
    last_seen_on DATE,
    dbt_created_on DATE,
    PRIMARY KEY (branch_key, study_key)
);

