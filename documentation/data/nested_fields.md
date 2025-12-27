
### Source: https://clinicaltrials.gov/data-api/about-api/study-data-structure


### NESTED FIELDS

## SponsorCollaboratorsModule

### sponsors

**Index Field(s):** 
- `protocolSection.sponsorCollaboratorsModule.leadSponsor`
- `protocolSection.sponsorCollaboratorsModule.collaborators[]`

**NOTE**: *sponsor name and class are scalar values and MUST be extracted directly*

**Object Type**: Simple dict (leadSponsor) / Array of dicts (collaborators)

**Description**: Organizations responsible for the study.

- **Lead Sponsor**: Exactly 1 per study. The organization or person who initiates the study and has authority and control over it.
- **Collaborators**: 0 to many. Other organizations providing support (funding, design, implementation, data analysis, reporting).


#### Fields

##### `name`
- **Description**: Name of the sponsoring entity or individual
- **Data Type**: Text
- **Limit**: 160 characters

##### `class`
- **Description**: Category of the sponsoring organization
- **Data Type**: Enum(AgencyClass)
- **Enum Values**:
  - `NIH` — National Institutes of Health
  - `FED` — Other Federal Agency
  - `OTHER_GOV` — Other Governmental (non-US)
  - `INDIV` — Individual
  - `INDUSTRY` — Industry/Pharmaceutical
  - `NETWORK` — Research Network
  - `AMBIG` — Ambiguous
  - `OTHER` — Other
  - `UNKNOWN` — Unknown



#### Model Mapping

- **Target Table**: `dim_sponsors`
- **Bridge Table**: `bridge_study_sponsors` 
- **Discriminator**: `is_lead_sponsor` (boolean) — added during transformation

---


## ConditionsModule 

### conditions

**Index Field(s):** `protocolSection.conditionsModule.conditions`

**Object Type**: Simple array

**Description**: The name(s) of the disease(s) or condition(s) studied in the clinical study, or the focus of the clinical study.

#### Model Mapping
- **Target Table**: `conditions`
- **Bridge Table**: `bridge_study_conditions` 
- **column_name**: `condition_name` 

---

### keywords

**Index Field(s):** `protocolSection.conditionsModule.keywords`

**Object Type**: Simple array

**Description**: Words or phrases that best describe the protocol. Keywords help users find studies in the database

#### Model Mapping
- **Target Table**: `keywords`
- **Bridge Table**: `bridge_study_keywords` 


---

## armsInterventionsModule 

### arm_groups

**Index Field:** `protocolSection.armsInterventionsModule.armGroups[]`

**Object Type**: Array of dicts

**Description**: Pre-specified group or subgroup of participants assigned to receive specific intervention(s) (or no intervention) according to protocol. For interventional studies only. Observational studies use Groups/Cohorts with the same structure but different semantics.

---

#### Fields

##### `label`
- **Description**: Short name used to identify the arm
- **Data Type**: Text
- **Limit**: 100 characters
- **Required**: Yes

##### `type`
- **Description**: The role of the arm in the clinical trial
- **Data Type**: Enum(ArmGroupType)
- **Required**: Yes
- **Enum Values**:
  - `EXPERIMENTAL` — Experimental
  - `ACTIVE_COMPARATOR` — Active Comparator
  - `PLACEBO_COMPARATOR` — Placebo Comparator
  - `SHAM_COMPARATOR` — Sham Comparator
  - `NO_INTERVENTION` — No Intervention
  - `OTHER` — Other

##### `description`
- **Description**: Additional descriptive information to differentiate this arm from others (including which interventions are administered)
- **Data Type**: Markup
- **Limit**: 999 characters
- **Required**: Conditional

#### Nested Fields
######  `interventionNames`
- **Description**: Names of interventions associated with this arm. References `interventions[].name` within the same study.
- **Data Type**: Array of Text
- **Limit**: 200 characters per name
- **Required**: Yes (for interventional studies)



#### Dimensional Model Mapping

- **Bridge Table**: `bridge_study_arm_groups` 
- **Surrogate Key**: `hash(study_key, label)`


---

### interventions

**Index Field:** `protocolSection.armsInterventionsModule.interventions[]`

**Object Type**: Array of dicts

**Description**: The intervention(s) studied in the clinical trial. For interventional studies, at least one required. For observational studies, specifies interventions/exposures of interest if any.

---

#### Fields

##### `name`
- **Description**: Brief descriptive name for the intervention. Non-proprietary name required if available.
- **Data Type**: Text
- **Limit**: 200 characters
- **Required**: Yes

##### `type`
- **Description**: General type of intervention
- **Data Type**: Enum(InterventionType)
- **Required**: Yes
- **Enum Values**:
  - `DRUG` — Drug (including placebo)
  - `DEVICE` — Device (including sham)
  - `BIOLOGICAL` — Biological/Vaccine
  - `PROCEDURE` — Procedure/Surgery
  - `RADIATION` — Radiation
  - `BEHAVIORAL` — Behavioral (e.g., psychotherapy, lifestyle counseling)
  - `GENETIC` — Genetic (gene transfer, stem cell, recombinant DNA)
  - `DIETARY_SUPPLEMENT` — Dietary Supplement (vitamins, minerals)
  - `COMBINATION_PRODUCT` — Combination Product (drug+device, biological+device, etc.)
  - `DIAGNOSTIC_TEST` — Diagnostic Test (imaging, in vitro)
  - `OTHER` — Other

##### `description`
- **Description**: Details sufficient to distinguish this intervention from similar ones (e.g., dosage form, dosage, frequency, duration for drugs)
- **Data Type**: Markup
- **Limit**: 1,000 characters
- **Required**: Yes

##### `armGroupLabels`
- **Description**: Labels of arm groups that receive this intervention. References `armGroups[].label` within the same study.
- **Data Type**: Array of Text
- **Required**: Yes (if multiple arms exist)

##### `otherNames`
- **Description**: Other current/former names or aliases (brand names, serial numbers)
- **Data Type**: Array of Text
- **Limit**: 200 characters per name
- **Required**: No

---

#### Dimensional Model Mapping

- **Target Table**: `dim_interventions`
- **Surrogate Key**: `hash(study_key, name, type)`
- **Bridge Table**: `bridge_study_interventions` (study_key, intervention_key)

---

### Arm <-->Intervention Relationship

**Source Data**: The API provides bidirectional references:
- `armGroups[].interventionNames` — intervention names per arm
- `interventions[].armGroupLabels` — arm labels per intervention

**Decision**: `armGroups[].interventionNames` as the source of truth for arm interventions and interventions[] as the source of truth interventions

**Rationale**:
1. Matches analytical workflow (arm → intervention, not reverse)
2. User-entered data may have inconsistencies between the two lists
3. Avoids reconciliation logic and potential mismatches from bidirectional data quality issues

**Implication**: Queries for "which arms use this intervention" require joining through `bridge_arm_interventions` from the arm side. We do not model the reverse relationship from `interventions[].armGroupLabels`.


### design_who_masked
**Index Field:** protocolSection.designModule.designInfo.maskingInfo.whoMasked

**Definition**: The party or parties involved in the clinical trial who are prevented from having knowledge of the interventions assigned to individual participants

**DataType**: [Enum(WhoMasked)]

**Source Values**:
* PARTICIPANT - Participant
* CARE_PROVIDER - Care Provider
* INVESTIGATOR - Investigator
* OUTCOMES_ASSESSOR - Outcomes Assessor


## contactsLocationsModule

#### locations

**Index Field(s):** `protocolSection.contactsLocationsModule.locations`

**Object Type**: Array of Dicts

**Description**: Participating facility in a clinical study

##### `facility`
- **Description**: ull name of the organization where the clinical study is being conducted
- **Data Type**: Text
- **Limit**: 254 characters

##### `city`
- **Description**: City
- **Data Type**: GeoName

##### `state`
- **Description**: State/Province. Required for U.S. locations (including territories of the United States)
- **Data Type**: GeoName

##### `zip`
- **Description**: ZIP/Postal Code. Required for U.S. locations (including territories of the United States)
- **Data Type**: GeoName

##### `geoPoint`
- **Description**: Lat and Lon
- **Data Type**: Dict

##### `status`
- **Description**: Individual Site Status 
- **Data Type**: Enum(RecruitmentStatus)
- **Enum Values**:
  - `ACTIVE_NOT_RECRUITING` - Active, not recruiting
  - `COMPLETED` - Completed
  - `ENROLLING_BY_INVITATION` - Enrolling by invitation
  - `NOT_YET_RECRUITING` - Not yet recruiting
  - `RECRUITING` - Recruiting
  - `SUSPENDED` - Suspended
  - `TERMINATED` - Terminated
  - `WITHDRAWN` - Withdrawn
  - `AVAILABLE` - Available

#### Nested Fields
##### `contacts`
###### modelled at central contacts level
- #### Model Mapping
- **Target Table**: `contacts`
- **Bridge Table**: `bridge_study_site_contacts` 


#### Model Mapping
- **Target Table**: `study_sites`
- **Bridge Table**: `bridge_study_sites` 




### central_contacts

**Index Field:** `protocolSection.contactsLocationsModule.centralContacts[]`

**Definition**: Contact person(s) for general enrollment questions across all study locations. Required if no facility-level contacts provided.

**Object Type**: Array of dicts

**Cardinality**: 0 to many (but at least one central OR facility contact required per study)

---

#### Fields

##### `name`
- **Description**: Name or title of contact person
- **Data Type**: Text
- **Required**: Yes

##### `role`
- **Description**: Role of the contact
- **Data Type**: Enum(CentralContactRole)
- **Values**: `CONTACT`, `PRINCIPAL_INVESTIGATOR`, etc.

##### `phone`
- **Description**: Telephone number (preferably toll-free)
- **Data Type**: Text
- **Required**: Yes

##### `phoneExt`
- **Description**: Phone extension
- **Data Type**: Text
- **Required**: No

##### `email`
- **Description**: Email address
- **Data Type**: Text
- **Required**: Yes

---

#### Dimensional Model Mapping

- **Target Table**: `dim_contacts`
- **Bridge Table**: `bridge_study_contacts` (study_key, contact_key)
- **Surrogate Key**: `hash(study_key, name, role, contact_type)`
- **Contact Type**: `CENTRAL` (discriminator)
- 

---

### site_contacts

**Index Field:** `protocolSection.contactsLocationsModule.locations[].contacts[]`

**Definition**: Contact person(s) for enrollment questions at a specific facility. Required if no central contact provided.

**Object Type**: Nested array of dicts (under locations)

**Cardinality**: 0 to many per site

---

#### Fields

(same as central_contacts)

---

#### Dimensional Model Mapping

- **Target Table**: `dim_contacts`
- **Surrogate Key**: `hash(site_key, name, role, contact_type)`
- **Contact Type**: `SITE` (discriminator)
- **Bridge Table**: `bridge_site_contacts` (site_key, contact_key)

    "central_contacts": {
        **Index Field:** "protocolSection.contactsLocationsModule.centralContacts",
        **Object_type**: "array_of_dicts",
        **Table_name**: "contacts",
        **Bridge_table_name**: "study_contacts",
        **Fields**: [
            ("name", "name"),
            ("role", "role"),
            ("email", "email"),
            ("phone", "phone"),
            ("phoneExt", "phoneExt")
        ],
        "transformer_method": "extract_contacts"
    },

    "overall_officials": {
        **Index Field:** "protocolSection.contactsLocationsModule.overallOfficials",
        **Object_type**: "array_of_dicts",
        **Table_name**: "investigators",
        **Bridge_table_name**: "study_investigators",
        **Fields**: [
            ("name", "name"),
            ("affiliation", "affiliation"),
            ("role", "role")
        ],
        "transformer_method": "extract_officials"
    },

    "primary_outcomes": {
        **Index Field:** "protocolSection.outcomesModule.primaryOutcomes",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_outcomes",
        **Fields**: [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "PRIMARY",
    },

    "secondary_outcomes": {
        **Index Field:** "protocolSection.outcomesModule.secondaryOutcomes",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_outcomes",
        **Fields**: [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "SECONDARY",
    },

    "other_outcomes": {
        **Index Field:** "protocolSection.outcomesModule.otherOutcomes",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_outcomes",
        **Fields**: [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "OTHER",
    },

    "references": {
        **Index Field:** "protocolSection.referencesModule.references",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_publications",
        "extract_fields": ["pmid", **Object_type**, "citation"],
    },

    "retractions": {
        **Index Field:** "protocolSection.referencesModule.retractions",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_retractions",
        **Fields**: [
            ("pmid", "pmid"),
            ("status", "status")
        ],
        "transformer_method": "extract_outcomes",
    },

    "see_also": {
        **Index Field:** "protocolSection.referencesModule.seeAlsoLinks",
        **Object_type**: "array_of_dicts",
        **Table_name**: "study_see_also",
        **Fields**: [
            ("label", "label"),
            ("url", "url")
        ],
        "transformer_method": "extract_see_also"
    },

    "phases": {
        **Index Field:** "protocolSection.designModule.phases",
        **Object_type**: "simple_array",
        **Table_name**: "phases",
        **Bridge_table_name**: "study_phases",
        **Field_name**: "phase",
        "transformer_method": "extract_study_phases"
    },


    "std_ages": {
        **Index Field:** "protocolSection.eligibilityModule.stdAges",
        **Object_type**: "simple_array",
        **Table_name**: "age_groups",
        **Bridge_table_name**: "study_age_groups",
        **Field_name**: "age_group",
        "transformer_method": "extract_age_group"
    },

    "ipd_info_types": {
        **Index Field:** "protocolSection.ipdSharingStatementModule.infoTypes",
        **Object_type**: "simple_array",
        **Table_name**: "ipd_info_types",
        **Bridge_table_name**: "study_ipd_info_types",
        **Field_name**: "info_type",
        "transformer_method": "extract_ipd_info_types"
    },
#NOT EVERYTHING NEEDS A BRIDGE
    "secondary_id_infos": {
        **Index Field:** "protocolSection.identificationModule.secondaryIdInfos",
        **Object_type**: "array_of_dicts",
        **Table_name**: "secondary_ids",
        **Bridge_table_name**: "study_secondary_ids",
        **Fields**: [
            ("id", "id"),
            (**Object_type**, **Object_type**),
            ("domain", "domain"),
            ("link", "link")
        ],
        "transformer_method": "extract_id_infos"

    },
    "nct_id_aliases": {
        **Index Field:** "protocolSection.identificationModule.nctIdAliases",
        **Object_type**: "simple_array",
        **Table_name**: "nct_aliases",
        **Bridge_table_name**: "study_nct_aliases",
        **Field_name**: "alias_nct_id",
        "transformer_method": "extract_nct_id_aliases"
    },

    # ===== DERIVED SECTION (MeSH) =====
    # CONDITION MESH TERMS


    "condition_mesh_terms": {
        **Index Field:** "derivedSection.conditionBrowseModule.meshes",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("id", "id"),
            ("term", "term")
        ],
        **Table_name**: "condition_mesh_terms",
        **Bridge_table_name**: "study_conditions_mesh",
        "is_primary": True,
        "transformer_method": "extract_condition_mesh"
    },

    "condition_mesh_ancestors": {
        **Index Field:** "derivedSection.conditionBrowseModule.ancestors",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("id", "id"),
            ("term", "term")
        ],
        **Table_name**: "condition_mesh_terms",
        **Bridge_table_name**: "study_conditions_mesh",
        "is_primary": False,
        "transformer_method": "extract_condition_mesh"
    },

    "intervention_mesh_terms": {
        **Index Field:** "derivedSection.interventionBrowseModule.meshes",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("id", "id"),
            ("term", "term")
        ],
        **Table_name**: "intervention_mesh_terms",
        **Bridge_table_name**: "study_interventions_mesh",
        "is_primary": True,
        "transformer_method": "extract_intervention_mesh"
    },

    "intervention_mesh_ancestors": {
        **Index Field:** "derivedSection.interventionBrowseModule.ancestors",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("id", "id"),
            ("term", "term")
        ],
        **Table_name**: "intervention_mesh_terms",
        **Bridge_table_name**: "study_interventions_mesh",
        "is_primary": False,
        "transformer_method": "extract_intervention_mesh"
    },

    "large_documents": {
        **Index Field:** "documentSection.largeDocumentModule.largeDocs",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("typeAbbrev", "typeAbbrev"),
            ("hasProtocol", "hasProtocol"),
            ("hasSap", "hasSap"),
            ("hasIcf", "hasIcf"),
            ("label", "label"),
            ("date", "date"),
            ("uploadDate", "uploadDate"),
            ("filename", "filename"),
            ("size", "size"),
        ],
        **Table_name**: "study_documents",
        "transformer_method": "extract_large_documents"
    },

    "unposted_events": {
        **Index Field:** "annotationSection.annotationModule.unpostedAnnotation.unpostedEvents",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            (**Object_type**, **Object_type**),
            ("date", "date"),
            ("dateUnknown", "dateUnknown"),
        ],
        **Table_name**: "unposted_events",
        **Bridge_table_name**: "study_unposted_events",
        "transformer_method": "extract_unposted_events"
    },

    "violation_events": {
        **Index Field:** "annotationSection.annotationModule.violationAnnotation.violationEvents",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            (**Object_type**, **Object_type**),
            ("description", "description"),
            ("creationDate", "creationDate"),
            ("issuedDate", "issuedDate"),
            ("releaseDate", "releaseDate"),
            ("postedDate", "postedDate"),
        ],
        **Table_name**: "violation_events",
        **Bridge_table_name**: "study_violation_events",
        "transformer_method": "extract_violation_events"
    },

    "removed_countries": {
        **Index Field:** "derivedSection.miscInfoModule.removedCountries",
        **Object_type**: "simple_array",
        **Table_name**: "countries",
        **Bridge_table_name**: "study_removed_countries",
        **Field_name**: "country",
        "transformer_method": "extract_removed_countries"
    },

    "submission_infos": {
        **Index Field:** "derivedSection.miscInfoModule.submissionTracking.submissionInfos",
        **Object_type**: "array_of_dicts",
        **Fields**: [
            ("releaseDate", "releaseDate"),
            ("unreleaseDate", "unreleaseDate"),
            ("unreleaseDateUnknown", "unreleaseDateUnknown"),
            ("resetDate", "resetDate"),
            ("mcpReleaseN", "mcpReleaseN")
        ],
        **Table_name**: "submission_tracking",
        **Bridge_table_name**: "study_submission_tracking",
        "transformer_method": "extract_submission_infos"
    },


    # # PARTICIPANT FLOW GROUPS
    # 'participant_flow_groups': {
    #     'path': 'resultsSection.participantFlowModule.groups',
    #     'type': 'array_of_dicts',
    #     'table_name': 'flow_groups',
    #     'bridge_table_name': 'study_flow_groups',
    #     'extract_fields': ['id', 'title', 'description']
    # },
    #
    # # PARTICIPANT FLOW PERIODS
    # 'participant_flow_periods': {
    #     'path': 'resultsSection.participantFlowModule.periods',
    #     'type': 'array_of_dicts',
    #     'table_name': 'flow_periods',
    #     'bridge_table_name': 'study_flow_periods',
    #     'extract_fields': ['title'],
    #     'nested_arrays': {
    #         'milestones': ['type', 'comment', 'achievements'],
    #         'dropWithdraws': ['type', 'comment', 'reasons']
    #     }
    # },
    #
    # # BASELINE GROUPS
    # 'baseline_groups': {
    #     'path': 'resultsSection.baselineCharacteristicsModule.groups',
    #     'type': 'array_of_dicts',
    #     'table_name': 'baseline_groups',
    #     'bridge_table_name': 'study_baseline_groups',
    #     'extract_fields': ['id', 'title', 'description']
    # },
    #
    # # BASELINE DENOMS
    # 'baseline_denoms': {
    #     'path': 'resultsSection.baselineCharacteristicsModule.denoms',
    #     'type': 'array_of_dicts',
    #     'table_name': 'baseline_denoms',
    #     'extract_fields': ['units'],
    #     'nested_arrays': {
    #         'counts': ['groupId', 'value']
    #     }
    # },
    #
    # # BASELINE MEASURES
    # 'baseline_measures': {
    #     'path': 'resultsSection.baselineCharacteristicsModule.measures',
    #     'type': 'array_of_dicts',
    #     'table_name': 'baseline_measures',
    #     'extract_fields': [
    #         'title', 'description', 'populationDescription',
    #         'paramType', 'dispersionType', 'unitOfMeasure',
    #         'calculatePct', 'denomUnitsSelected'
    #     ],
    #     'nested_arrays': {
    #         'denoms': ['units', 'counts'],
    #         'classes': ['title', 'denoms', 'categories']
    #     }
    # },
    #
    # # OUTCOME MEASURES
    # 'outcome_measures_results': {
    #     'path': 'resultsSection.outcomeMeasuresModule.outcomeMeasures',
    #     'type': 'array_of_dicts',
    #     'table_name': 'outcome_measure_results',
    #     'extract_fields': [
    #         'type', 'title', 'description', 'populationDescription',
    #         'reportingStatus', 'anticipatedPostingDate',
    #         'paramType', 'dispersionType', 'unitOfMeasure',
    #         'calculatePct', 'timeFrame', 'typeUnitsAnalyzed',
    #         'denomUnitsSelected'
    #     ],
    #     'nested_arrays': {
    #         'groups': ['id', 'title', 'description'],
    #         'denoms': ['units', 'counts'],
    #         'classes': ['title', 'denoms', 'categories'],
    #         'analyses': [
    #             'groupIds', 'paramType', 'paramValue',
    #             'dispersionType', 'dispersionValue',
    #             'statisticalMethod', 'statisticalComment',
    #             'pValue', 'pValueComment',
    #             'ciNumSides', 'ciPctValue', 'ciUpperLimit', 'ciLowerLimit',
    #             'estimateComment', 'testedNonInferiority',
    #             'nonInferiorityType', 'nonInferiorityComment',
    #             'otherAnalysisDescription', 'groupDescription'
    #         ]
    #     }
    # },
    #
    # # ADVERSE EVENT GROUPS
    # 'adverse_event_groups': {
    #     'path': 'resultsSection.adverseEventsModule.eventGroups',
    #     'type': 'array_of_dicts',
    #     'table_name': 'ae_groups',
    #     'bridge_table_name': 'study_ae_groups',
    #     'extract_fields': [
    #         'id', 'title', 'description',
    #         'deathsNumAffected', 'deathsNumAtRisk',
    #         'seriousNumAffected', 'seriousNumAtRisk',
    #         'otherNumAffected', 'otherNumAtRisk'
    #     ]
    # },
    #
    # # SERIOUS ADVERSE EVENTS
    # 'serious_adverse_events': {
    #     'path': 'resultsSection.adverseEventsModule.seriousEvents',
    #     'type': 'array_of_dicts',
    #     'table_name': 'adverse_events',
    #     'extract_fields': [
    #         'term', 'organSystem', 'sourceVocabulary',
    #         'assessmentType', 'notes'
    #     ],
    #     'nested_arrays': {
    #         'stats': ['groupId', 'numEvents', 'numAffected', 'numAtRisk']
    #     },
    #     'severity': 'SERIOUS'
    # },
    #
    # # OTHER ADVERSE EVENTS
    # 'other_adverse_events': {
    #     'path': 'resultsSection.adverseEventsModule.otherEvents',
    #     'type': 'array_of_dicts',
    #     'table_name': 'adverse_events',
    #     'extract_fields': [
    #         'term', 'organSystem', 'sourceVocabulary',
    #         'assessmentType', 'notes'
    #     ],
    #     'nested_arrays': {
    #         'stats': ['groupId', 'numEvents', 'numAffected', 'numAtRisk']
    #     },
    #     'severity': 'OTHER'
    # }

    }
