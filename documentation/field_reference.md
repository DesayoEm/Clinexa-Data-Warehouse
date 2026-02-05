
### Source: https://clinicaltrials.gov/data-api/about-api/study-data-structure


## identificationModule

- **Index Field:** `protocolSection.identificationModule`
- **Description**: Study Identification

### Scalar fields

##### nctId
- **Definition**: The unique identification code given to each clinical study upon registration at ClinicalTrials.gov. 
      The format is "NCT" followed by an 8-digit number.  Also known as ClinicalTrials.gov Identifier
- **Limit**: 11 characters (growing).

##### briefTitle
- **Definition**: A short title of the clinical study written in language intended for the lay public.
- **Data Type**: Text
- **Limit**: 300 characters.


##### officialTitle
- **Definition**:  The title of the clinical study, corresponding to the title of the protocol.
- **Data Type**: Text
- **Limit**: 600 characters

##### acronym
- **Definition**: An acronym or abbreviation used publicly to identify the clinical study, if any.
- **Data Type**: Text
- **Limit**: 14 characters.

##### orgStudyIdInfo.id -- org_study_id 
- **Definition**: Organization's Unique Protocol Identification Number
- **Data Type**: Text
- **Data Type**: Text
- **Limit**: 30 characters.
- 

### Non-scalar fields
##### `nctIdAliases`
- **Definition**: Identifier(s) that are considered "Obsolete" or "Duplicate".
- **Data Type**: nct[]


##### `SecondaryIdInfo`
- **Definition**: An identifier(s) (ID), if any, other than the organization's Unique Protocol Identification Number or the NCT number that is assigned to the clinical study.
- **Data Type**: SecondaryIdInfo[]
- **Fields**: [id, type, domain, link]


---

## statusModule

- **Index Field:** `protocolSection.statusModule`
- **Description**: Study Status

### Scalar fields

##### statusVerifiedDate
- **Index Field:** `protocolSection.statusModule.statusVerifiedDate`
- **Definition**: The date on which the responsible party last verified the clinical study information in the entire ClinicalTrials.gov record for the clinical study, even if no additional or updated information is being submitted.
- **DataType**: PartialDate 


##### lastKnownStatus
- **Definition**: A study on ClinicalTrials.gov whose last known status was recruiting; not yet recruiting; or active, not recruiting but that has passed its completion date, and the status has not been last verified within the past 2 years.
- **DataType**: Enum (Status)

**Source Values**:

- * ACTIVE_NOT_RECRUITING - Active, not recruiting
- * COMPLETED - Completed
- * ENROLLING_BY_INVITATION - Enrolling by invitation
- * NOT_YET_RECRUITING - Not yet recruiting
- * RECRUITING - Recruiting
- * SUSPENDED - Suspended
- * TERMINATED - Terminated
- * WITHDRAWN - Withdrawn
- * AVAILABLE - Available
- * NO_LONGER_AVAILABLE - No longer available
- * TEMPORARILY_NOT_AVAILABLE - Temporarily not available
- * APPROVED_FOR_MARKETING - Approved for marketing
- * WITHHELD - Withheld
- * UNKNOWN - Unknown status

##### overallStatus
- **Definition**: The recruitment status for the clinical study as a whole, based upon the status of the individual sites.
   If at least one facility in a multi-site clinical study has an Individual Site Status of "Recruiting," then the Overall Recruitment Status for the study must be "Recruiting."

- **DataType**: Enum (Status)
- 
**Enum Values**:

- * ACTIVE_NOT_RECRUITING - Active, not recruiting
- * COMPLETED - Completed
- * ENROLLING_BY_INVITATION - Enrolling by invitation
- * NOT_YET_RECRUITING - Not yet recruiting
- * RECRUITING - Recruiting
- * SUSPENDED - Suspended
- * TERMINATED - Terminated
- * WITHDRAWN - Withdrawn
- * AVAILABLE - Available
- * NO_LONGER_AVAILABLE - No longer available
- * TEMPORARILY_NOT_AVAILABLE - Temporarily not available
- * APPROVED_FOR_MARKETING - Approved for marketing
- * WITHHELD - Withheld
- * UNKNOWN - Unknown status


### whyStopped
- **Index Field:** `protocolSection.statusModule.whyStopped`
- **Definition**: A brief explanation of the reason(s) why such clinical study was stopped (for a clinical study that is "Suspended," "Terminated," or "Withdrawn" prior to its planned completion as anticipated by the protocol).
- **DataType**: Markup
- **Limit**: 250 characters


##### expandedAccessInfo.hasExpandedAccess -- has_expanded_access
- **Definition**:  Whether there is expanded access to the investigational product for patients who do not qualify for enrollment in a clinical trial.
- **DataType**: Boolean 

##### expandedAccessInfo.nctId -- expanded_access_nct
- **Definition**:  If expanded access is available, the NCT number of the expanded access record.
- **DataType**: Boolean 


##### expandedAccessInfo.statusForNctId -- expanded_access_status
- **Definition**: Recruitment status of the EA study that's linked to INT/OBS
- **DataType**: Boolean 


##### startDateStruct.date -- start_date
- **Definition**: The estimated date on which the clinical study will be open for recruitment of participants, or the actual date on which the first participant was enrolled.
- **DataType**: PartialDate 

##### startDateStruct.type -- start_date_type
- **Definition**: A study on ClinicalTrials.gov whose last known status was recruiting; not yet recruiting; or active, not recruiting but that has passed its completion date, and the status has not been last verified within the past 2 years.
- **DataType**: Enum (DateType)
- 
**Source Values**:
- * ACTUAL - Actual
- * ESTIMATED - Estimated


##### completionDateStruct.date -- completion_date
- **Definition**: The date the final participant was examined or received an intervention for purposes of final collection of data for the primary and secondary outcome measures and adverse events (for example, last participant’s last visit), whether the clinical study concluded according to the pre-specified protocol or was terminated.
- **DataType**: PartialDate  


##### completionDateStruct.type -- completion_date_type
- **Definition**: A study on ClinicalTrials.gov whose last known status was recruiting; not yet recruiting; or active, not recruiting but that has passed its completion date, and the status has not been last verified within the past 2 years.
- **DataType**: Enum (DateType)
- 
**Source Values**:
- * ACTUAL - Actual
- * ESTIMATED - Estimated

##### studyFirstSubmitDate
- **Definition**: The date on which the study sponsor or investigator first submitted a study record to ClinicalTrials.gov. There is typically a delay of a few days between the first submitted date and the record's availability on ClinicalTrials.gov (the first posted date).
- **DataType**: NormalizedDate  


##### lastUpdateSubmitDate
- **Index Field:** `protocolSection.statusModule.`
- **Definition**: The most recent date on which the study sponsor or investigator submitted changes to a study record to ClinicalTrials.gov. There is typically a delay of a few days between the last update submitted date and when the date changes are posted on ClinicalTrials.gov (the last update posted date).
It is the responsibility of the sponsor or investigator to ensure that the study record is consistent with the NLM QC review criteria.
- **DataType**: NormalizedDate  


##### lastUpdatePostDateStruct.date -- last_updated
- **Index Field:** `protocolSection.statusModule.`
- **Definition**: The most recent date on which changes to a study record were made available on ClinicalTrials.gov.
There may be a delay between when the changes were submitted to ClinicalTrials.gov by the study's sponsor or investigator (the last update submitted date) and the last update posted date.
- **DataType**: NormalizedDate 

---

## SponsorCollaboratorsModule

- **Index Field:** `protocolSection.sponsorCollaboratorsModule`
- **Description**: Organizations responsible for the study.

### Scalar fields

##### responsibleParty
- **Definition**: An indication of whether the responsible party is the sponsor, the sponsor-investigator, or a principal investigator designated by the sponsor to be the responsible party
- **DataType**: ResponsiblePartyType

**Enum Values**: 
- * SPONSOR - Sponsor
- * PRINCIPAL_INVESTIGATOR - Principal Investigator
- * SPONSOR_INVESTIGATOR - Sponsor-Investigator

    
### Non-scalar fields
#### `sponsor`

- **Definition**: Name of the sponsoring entity or individual
- **Data Type**: Sponsor
- **Fields**: [name,class]


##### `collaborators`

- **Definition**: Other organizations, if any, providing support. Support may include funding, design, implementation, data analysis or reporting.
- **Data Type**: Sponsor[]
- **Fields**: [name,class]


- **Lead Sponsor**: Exactly 1 per study. The organization or person who initiates the study and has authority and control over it.
- **Collaborators**: 0 to many. Other organizations providing support (funding, design, implementation, data analysis, reporting).

---

## oversightModule

- **Index Field:** `protocolSection.oversightModule`
- **Description**: Oversight.

### Scalar fields

##### oversightHasDmc
- **Definition**: Whether a data monitoring committee has been appointed for this study.
    The data monitoring committee (board) is a group of independent scientists who are appointed to monitor the safety and scientific integrity of a human research intervention, and to make recommendations to the sponsor regarding the stopping of the trial for efficacy, for harms or for futility.
- **DataType**: Boolean (Yes/No)


##### isFdaRegulatedDrug
- **Definition**: Whether a clinical study is studying a drug product (including a biological product) subject to section 505 of the Federal Food, Drug, and Cosmetic Act or to section 351 of the Public Health Service Act.
- **DataType**: Boolean (Yes/No)


##### isFdaRegulatedDevice
- **Definition**: Whether clinical study is studying a device product subject to section 510(k), 515, or 520(m) of the Federal Food, Drug, and Cosmetic Act.
- **DataType**: Boolean (Yes/No)


##### isUnapprovedDevice
- **Definition**: Indication that at least one device product studied in the clinical study has not been previously approved or cleared by the U.S. Food and Drug Administration (FDA) for one or more uses.
- **DataType**: Boolean (true/false)



##### is_us_export
- **Index Field:** `protocolSection.oversightModule.isUsExport`
- **Definition**: Whether any drug product (including a biological product) or device product studied in the clinical study is manufactured in the United States or one of its territories and exported for study in a clinical study in another country.
Required if U.S. FDA-regulated Drug and/or U.S. FDA-regulated Device is "Yes," U.S. FDA IND or IDE is "No", and Facility Information does not include at least one U.S. location.
- **DataType**: Boolean (Yes/No)

---


## descriptionModule

- **Index Field:** `protocolSection.descriptionModule`
- **Description**: Study Description

### Scalar fields
##### briefSummary
- **Index Field:** `protocolSection.descriptionModule.briefSummary`
- **Definition**: A short description of the clinical study, including a brief statement of the clinical study's hypothesis, written in language intended for the lay public.
- **Limit**: 5,000  characters.


##### detailedDescription
- **Index Field:** `protocolSection.descriptionModule.detailedDescription`
- **Definition**: Extended description of the protocol, including more technical information (as compared to the Brief Summary), if desired. Does not include the entire protocol
- **Limit**: 32,000  characters.

---

## ConditionsModule 

- **Index Field:** `protocolSection.conditionsModule`
- **Description**: The name(s) of the disease(s) or condition(s) studied in the clinical study, or the focus of the clinical study.

### Non-scalar fields
#####  `conditions`
- **Object Type**: text[]

##### `keywords`

- **Description**: Words or phrases that best describe the protocol. Keywords help users find studies in the database
- **Object Type**: text[]

---


## designModule 

- **Index Field:** `protocolSection.designModule`
- **Description**: Study Design

### Scalar fields

##### studyType
- **Definition**: Study type
- **DataType**: Enum(StudyType)

**Enum Values**: 
- * EXPANDED_ACCESS - Expanded Access
- * INTERVENTIONAL - Interventional
- * OBSERVATIONAL - Observational


##### patientRegistry
- **Definition**: A type of observational study that collects information about patients' medical conditions and/or treatments to better understand how a condition or treatment affects patients in the real world.
- **DataType**: Boolean (True/False)

##### enrollmentInfo.type -- enrollment_type
- *Definition**: Enrollment type
- **DataType**: Enum(EnrollmentType)

**Enum Values**: 
- * ACTUAL - Actual
- * ESTIMATED - Estimated


##### enrollmentInfo.count -- enrollment_count
- **Definition**: The estimated total number of participants to be enrolled (target number) or the actual total number of participants that are enrolled in the clinical study.
- **DataType**: Integer


##### designInfo.allocation -- design_allocation
- **Definition**: The method by which participants are assigned to arms in a clinical trial.
- **DataType**: Enum(DesignAllocation)

**Enum Values**: 
- * RANDOMIZED - Randomized
- * NON_RANDOMIZED - Non-Randomized
- * NA - N/A


##### designInfo.interventionModel -- design_intervention_model
- **Definition**: The strategy for assigning interventions to participants.
- **DataType**: Enum(InterventionalAssignment)

**Enum Values**:
- * SINGLE_GROUP - Single Group Assignment
- * PARALLEL - Parallel Assignment
- * CROSSOVER - Crossover Assignment
- * FACTORIAL - Factorial Assignment
- * SEQUENTIAL - Sequential Assignment


##### designInfo.interventionModelDescription -- design_intervention_model_desc 
- **Definition**: details about the Interventional Study Model.
- **Limit**: 1,000 characters
- **DataType**: Markup


##### designInfo.primaryPurpose -- design_primary_purpose
- **Definition**: The main objective of the intervention(s) being evaluated by the clinical trial
- **Limit**: 1,000 characters
- **DataType**: Enum(PrimaryPurpose)

**Enum Values**:
- * TREATMENT - Treatment
- * PREVENTION - Prevention
- * DIAGNOSTIC - Diagnostic
- * ECT - Educational/Counseling/Training
- * SUPPORTIVE_CARE - Supportive Care
- * SCREENING - Screening
- * HEALTH_SERVICES_RESEARCH - Health Services Research
- * BASIC_SCIENCE - Basic Science
- * DEVICE_FEASIBILITY - Device Feasibility
- * OTHER - Other


##### designInfo.observationalModel -- design_observational_model
- **Definition**: Primary strategy for participant identification and follow-up.
- **DataType**: Enum(ObservationalModel)

**Enum Values**:
-  COHORT - Cohort
- * CASE_CONTROL - Case-Control
- * CASE_ONLY - Case-Only
- * CASE_CROSSOVER - Case-Crossover
- * ECOLOGIC_OR_COMMUNITY - Ecologic or Community
- * FAMILY_BASED - Family-Based
- * DEFINED_POPULATION - Defined Population
- * NATURAL_HISTORY - Natural History
- * OTHER - Other


##### designInfo.timePerspective -- design_time_perspective
- **Definition**: Temporal relationship of observation period to time of participant enrollment.
- **DataType**: Enum(DesignTimePerspective)

**Enum Values**:
- * RETROSPECTIVE - Retrospective
- * PROSPECTIVE - Prospective
- * CROSS_SECTIONAL - Cross-Sectional
- * OTHER - Other

##### designInfo.masking -- design_masking
- **Definition**: The party or parties involved in the clinical trial who are prevented from having knowledge of the interventions assigned to individual participants
- **DataType**: Enum(DesignMasking)

**Enum Values**:
- * NONE - None (Open Label)
- * SINGLE - Single
- * DOUBLE - Double
- * TRIPLE - Triple
- * QUADRUPLE - Quadruple


##### bioSpec.retention -- biospec_retention
- **Definition**:Whether samples of material from research participants are retained in a biorepository
- **DataType**: Enum(BioSpecRetention)

**Enum Values**:
- * NONE_RETAINED - None Retained
- * SAMPLES_WITH_DNA - Samples With DNA
- * SAMPLES_WITHOUT_DNA - Samples Without DNA


##### bioSpec.description -- biospec_desc
- **Definition**:Specify all types of biospecimens to be retained (e.g., whole blood, serum, white cells, urine, tissue).
- **Limit**: 1,000 characters.

---
## armsInterventionsModule 

- **Index Field:** `protocolSection.armsInterventionsModule`
- **Description**: A description of each arm of the clinical trial that indicates its role in the clinical trial

### Non-scalar fields
##### `ArmGroup`

- **Object Type**: ArmGroup[]
- **Description**: Pre-specified group or subgroup of participants assigned to receive specific intervention(s) (or no intervention) according to protocol. For interventional studies only. Observational studies use Groups/Cohorts with the same structure but different semantics.
- **Fields**: [label,type, description, [interventionNames]]


##### `intervention`

- **Description**: The intervention(s) studied in the clinical trial. For interventional studies, at least one required. For observational studies, specifies interventions/exposures of interest if any.
- **Object Type**: Intervention[]
- **Fields**: [name,type, description, [otherNames]]


##### Arm <-->Intervention Relationship

**Source Data**: The API provides bidirectional references:
- `armGroups[].interventionNames` - intervention names per arm
- `interventions[].armGroupLabels` - arm labels per intervention

`armGroups[].interventionNames` as the source of truth for arm interventions and interventions[] as the source of truth interventions

**Rationale**:
1. Matches analytical workflow (arm -> intervention, not reverse)
2. User-entered data may have inconsistencies between the two lists
3. Avoids reconciliation logic and potential mismatches from bidirectional data quality issues

**Implication**: Queries for "which arms use this intervention" require joining through `bridge_arm_interventions` from the arm side. We do not model the reverse relationship from `interventions[].armGroupLabels`.


---

## outcomesModule

- **Index Field:** `protocolSection.outcomesModule`
- **Description**: Outcome Measures


### Non-scalar fields
### `primaryOutcomes`

- **Definition**:  A description of each primary outcome measure (or for observational studies, specific key measurement[s] or observation[s] used to describe patterns of diseases or traits or associations with exposures, risk factors or treatment).
- **Data Type**: Outcome[]
- **Fields**: [measure,description, timeFrame]
#### Fields


### `secondaryOutcomes`

- **Definition**:  A description of each secondary outcome measure (or for observational studies, specific key measurement[s] or observation[s] used to describe patterns of diseases or traits or associations with exposures, risk factors or treatment).
- **Data Type**: Outcome[]
- **Fields**: [measure,description, timeFrame]


### `otherOutcomes`

- **Definition**:  A description of each other outcome measure (or for observational studies, specific key measurement[s] or observation[s] used to describe patterns of diseases or traits or associations with exposures, risk factors or treatment).
- **Data Type**: Outcome[]
- **Fields**: [measure,description, timeFrame]

---


## eligibilityModule 

- **Index Field:** `protocolSection.eligibilityModule`
- **Description**: Eligibility
- 
### Scalar fields

##### eligibilityCriteria
- **Definition**: A limited list of criteria for selection of participants in the clinical study, provided in terms of inclusion and exclusion criteria and suitable for assisting potential participants in identifying clinical studies of interest. 
Bulleted list for each criterion below the headers "Inclusion Criteria" and "Exclusion Criteria".

- **Limit**: 20,000 characters.


##### healthyVolunteers
- **Definition**: Indication that participants who do not have a disease or condition, or related conditions or symptoms, under study in the clinical study are permitted to participate in the clinical study.
- **DataType**: boolean 

##### sex
- **Definition**: The sex of the participants eligible to participate in the clinical study
- **DataType**: Enum(Sex)

**Enum Values**:
- * FEMALE - Female
- * MALE - Male
- * ALL - All

    
##### minimumAge
- **Definition**: The numerical value, if any, for the minimum age a potential participant must meet to be eligible for the clinical study.
- **DataType**: NormalizedTime 

**Unit of Time**
- * Years
- * Months
- * Weeks
- * Days
- * Hours
- * Minutes
- * N/A (No limit)

##### maximumAge
- **Definition**: The numerical value, if any, for the maximum age a potential participant must meet to be eligible for the clinical study.
- **DataType**: NormalizedTime 

**Unit of Time**
- * Years
- * Months
- * Weeks
- * Days
- * Hours
- * Minutes
- * N/A (No limit)


##### studyPopulation
- **Definition**: A description of the population from which the groups or cohorts will be selectedm(for example, primary care clinic, community sample, residents of a certain town).
- **For observational studies only**
- **DataType**: Markup
- *Limit**: 1,000 characters.


##### samplingMethod
- **Definition**: The method used for the sampling approach
- **For observational studies only**
- **DataType**: Enum (SamplingMethod)

**Enum Values**:
- * PROBABILITY_SAMPLE - Probability Sample
- * NON_PROBABILITY_SAMPLE - Non-Probability Sample

---

## contactsLocationsModule

- **Index Field:** `protocolSection.contactsLocationsModule`
- **Description**:Contacts, Locations, and Investigator Information

### Non-scalar fields

##### `centralContacts`
- **Definition**: Contact person(s) for general enrollment questions across all study locations. Required if no facility-level contacts provided.
- **Object Type**: centralContacts[]
- **Cardinality**: 0 to many (but at least one central OR facility contact required per study)
- **Fields**: [name,role, phone, phoneExt, email]


##### `locations`

- **Object Type**: Location[]
- **Description**: Participating facility in a clinical study
- **Fields**: [facility,status, city, state, zip, geoPoint:[lat,lon], contacts:{}]

NOTE: Contacts are stored denormalized as JSON since not used for filtering/analysis.

---

## referencesModule

- **Index Field:** `protocolSection.referencesModule`
- **Description**:Citations to publications related to the protocol

### Non-scalar fields

##### references
- **Definition**: Citations to publications related to the protocol
- **DataType**: Reference[]
- **Fields**: [pmid, type, citations]


##### seeAlsoLinks
- **Definition**:  A website directly relevant to the protocol 
- **DataType**: SeeAlsoLink[]
- **Fields**: [label, url]


##### availIpds
- **Definition**: Available individual participant data (IPD) sets and supporting information that are being shared for the study.
- **DataType**: AvailIpd[]
- **Fields**: [id, type, url, comment]


---

## ipdSharingStatementModule

- **Index Field:** `protocolSection.ipdSharingStatementModule`
- **Description**:Plan to make individual participant data (IPD) collected in the study

### Scalar fields

##### ipdSharing
- **Definition**: Indication  whether there is a plan to make individual participant data (IPD) collected in this study, including data dictionaries, available to other researchers (typically after the end of the study).
- **DataType**: Enum (IpdSharing)

**Source Values**:
- * YES - Yes
- * NO - No
- * UNDECIDED - Undecided


##### description
- **Definition**: What specific individual participant data sets are to be shared (for example, all collected IPD, all IPD that underlie results in a publication).
If the Plan to Share IPD is "No" or "Undecided," an explanation may be provided for why IPD will not be shared or why it is not yet decided.
- **DataType**: Markup
- **Limit**: 1,000 characters.


##### timeFrame
- **Definition**: A description of when the IPD and any additional supporting information will become available and for how long, including the start and end dates or period of availability.
This may be provided as an absolute date (for example, starting in January 2025) or as a date relative to the time when summary data are published or otherwise made available (for example, starting 6 months after publication).
- **DataType**: Markup
- **Limit**: 1,000 characters.


##### accessCriteria
- **Definition**: Describe by what access criteria IPD and any additional supporting information will be shared, including with whom, for what types of analyses, and by what mechanism.
Information about who will review requests and criteria for reviewing requests may also be provided.
- **DataType**: Markup
- **Limit**: 1,000 characters.


##### url
- **Definition**: The web address, if any, used to find additional information about the plan to share IPD.
- **DataType**: Text
- **Limit**: 3,999 characters.

---

## participantFlowModule

- **Index Field:** `resultsSection.participantFlowModule`
- **Definition**: Information to document the progress of research participants through each stage of a study

### Non-scalar fields

##### `groups`
- **Definition**: Arms or groups for describing the flow of participants through the clinical study.
- **DataType**: FlowGroup[]
- **Fields**: [id, title, description]


##### `periods`
- **Definition**:  Discrete stages of a clinical study during which numbers of participants at specific significant events or points of time are reported.
- **DataType**: FlowPeriod[]
- **Fields**: [title, milestones:[type, comment, 
              achievements:[groupId,comment, numSubjects, numUnits],
              dropWithdraws:[type,comment, reasons: [groupId, comment, numSubjects]]],
              ]


**FLOW PERIOD DUPLICATE HANDLING**

### Issue
- Some studies contain duplicate period entries with the same title  but different participant counts.

- **Resolution**:Aggregate duplicate (study, period, event, group) combinations  by SUMMING num_subjects. 

- This assumes multiple entries represent cumulative enrollment or separate cohorts within the same period.

- **Limitation:** If entries represent corrections (not additions), totals may  be inflated.


----


## outcomeMeasuresModule

- **Index Field:** `protocolSection.outcomesModule`
- **Description**: Outcome Measures

### Non-scalar fields

##### `outcomeMeasures`
- **Definition**: "Outcome measure" means a pre-specified measurement that is used to determine the effect of an experimental variable on participants in the study. 
- **DataType**: OutcomeMeasure[]
- **Fields**: [title, description, populationDescription, reportingStatus, anticipatedPostingDate,paramType
              dispersionType, unitOfMeasure, calculatePct, timeFrame, typeUnitsAnalyzed, denomUnitsSelected
               ]


##### `groups`
- **Definition**:  Arms or comparison groups in the study,
- **DataType**: OutcomeGroup[]
- **Fields**: [id, title, description]
- 

##### `denoms`
- **Definition**:  Analysis units and counts
- **DataType**: Denom[]
- **Fields**: [units, [counts]]

##### `classes`
- **Definition**:  Arms or comparison groups in the study, 
- including all arms or comparison groups based on the pre-specified protocol and/or statistical analysis plan.
- **DataType**: MeasureClass[]
- **Fields**: [units, counts[groupId, value]]



---
## adverseEventsModule

- **Index Field:** `resultsSection.adverseEventsModule`
- **Definition**: Information for completing three tables summarizing adverse events.


### Non-scalar fields

##### eventGroups
- **Definition**: Arms or comparison groups in the study,
- **DataType**: EventGroup[] 
- **Fields**: [id, title, description, deathsNumAtRisk, seriousNumAffected, seriousNumAtRisk ,otherNumAffected]


##### seriousEvents
- **Definition**: A table of all anticipated and unanticipated serious adverse events, grouped by organ system, with the number and frequency of such events by arm or comparison group of the clinical study.
- **DataType**: AdverseEvent[]
- **Fields**: [term, organSystem, sourceVocabulary, assessmentType, notes, stats ,otherNumAffected]


##### otherEvents
- **Definition**:Other (Not Including Serious) Adverse Events - similar to Serious AE
- **DataType**: AdverseEvent[]
- **Fields**: [term, organSystem, sourceVocabulary, assessmentType, notes, stats ,otherNumAffected]

---


## moreInfoModule

- **Index Field:** `protocolSection.moreInfoModule`

### Scalar fields

##### certainAgreement.piSponsorEmployee
- **Definition**: Whether the principal investigator is an employee of the sponsor.
- **DataType**: Boolean (Yes/ No)
- **Limit**: 500 characters.


##### certainAgreement.restrictiveAgreement
- **Index Field:** `resultsSection.moreInfoModule.`
- **Definition**: Whether there exists any agreement (other than an agreement solely to comply with applicable provisions of law protecting the privacy of participants participating in the clinical study) between the sponsor or its agent and the principal investigator (PI) that restricts in any manner the ability of the PI to discuss the results of the clinical study at a scientific meeting or any other public or private forum or to publish in a scientific or academic journal the results of the clinical study, after the
- **DataType**: Boolean (Yes/ No)
- **Limit**: 500 characters.


##### certainAgreement.restrictionType
- **Definition**: Additional information about the results disclosure restriction. 

The type that represents the most restrictive of the agreements is used If there are varying agreements
- **DataType**: Enum (AgreementRestrictionType)
**Source Values**:

- * LTE60 - LTE60
- * GT60 - GT60
- * OTHER - OTHER


##### certainAgreement.otherDetails
- **Definition**: The type of agreement including any provisions allowing the sponsor to require changes, ban the communication, or extend an embargo if "Other" disclosure agreement is selected on `resultsSection.moreInfoModule.certainAgreement.restrictionType`
- **DataType**: Markup
**Limit**: 500 characters.

  
##### pointOfContact.title
- **Index Field:** `resultsSection.moreInfoModule.`
- **Definition**: The person who is designated the point of contact.
This may be a specific person's name (for example, Dr. Jane Smith) or a position title (for example, Director of Clinical Trials).
- **DataType**: Text


##### pointOfContact.organization
- **Definition**: Full name of the designated individual's organizational affiliation.
- **DataType**: Text


##### pointOfContact.email
- **Definition**: Electronic mail address of the designated individual.
- **DataType**: Text


##### pointOfContact.phone
- **Definition**: Office phone number of the designated individual. Format 123-456-7890 within the United States and Canada. 

If outside the United States and Canada, the full phone number, including the country code is provided.

**DataType**: Text


##### pointOfContact.phoneExt
- **Index Field:** `resultsSection.moreInfoModule.`
- **Definition**: Phone extension, if needed


### limitationsAndCaveats.description
- **Index Field:** `resultsSection.moreInfoModule.`
- **Definition**: Significant limitations of the study. 
Such limitations may include not reaching the target number of participants needed to achieve target power and statistically reliable results or technical problems with measurements leading to unreliable or uninterpretable data.
- **DataType**: Markup
- **Limit**: 500 characters.

---















## conditionBrowseModule
- **Index Field(s):** `derivedSection.conditionBrowseModule`
- **Description**: Support for "Search By Topic"
- **DataType**: BrowseModule

### Non-scalar fields

### `meshes`
- **Description**: MeSH terms of Condition/Diseases field
- **DataType**: Mesh[]
- **Fields**: [id, term]


### `ancestors`
- **Description**: Ancestor (higher level and more broad) terms of Condition MeSH terms in MeSH Tree hierarchy
- **DataType**: Mesh[]
- **Fields**: [id, term]


### `browseLeaves`
- **Description**: Leaf browsing topics for Condition field
- **DataType**: BrowseLeaf[]
- **Fields**: [id, asFound, relevance]


### `browseBranches`
- **Description**: Branch browsing topics for Condition field
- **DataType**: BrowseBranch[]
- **Fields**: [abbrev, name, relevance]

---



## conditionBrowseModule
- **Index Field(s):** `derivedSection.interventionBrowseModule`
- **Description**: Support for "Search By Topic"
- **DataType**: BrowseModule

### Non-scalar fields

### `meshes`
- **Description**: MeSH terms of intervention field
- **DataType**: Mesh[]
- **Fields**: [id, term]


### `ancestors`
- **Description**: Ancestor (higher level and more broad) terms of intervention MeSH terms in MeSH Tree hierarchy
- **DataType**: Mesh[]
- **Fields**: [id, term]


### `browseLeaves`
- **Description**: Leaf browsing topics for intervention field
- **DataType**: BrowseLeaf[]
- **Fields**: [id, asFound, relevance]


### `browseBranches`
- **Description**: Branch browsing topics for intervention field
- **DataType**: BrowseBranch[]
- **Fields**: [abbrev, name, relevance]

---

## participant flow

### flow_pre_assignment_details
- **Index Field:** `resultsSection.participantFlowModule.preAssignmentDetails`
- **Definition**: T Description of significant events in the study (for example, wash out, run-in) that occur after participant enrollment, but prior to assignment of participants to an arm or group, if any
- **DataType**: Text
- **Limit**: 500 characters.


### flow_recruitment_details
- **Index Field:** `resultsSection.participantFlowModule.recruitmentDetails`
- **Definition**: Key information relevant to the recruitment process for the overall study, 
      such as dates of the recruitment period and types of location (For example, medical clinic), to provide context.
- **DataType**: Text
- **Limit**: 500 characters.


### flow_type_units_analysed
- **Index Field:** `resultsSection.participantFlowModule.typeUnitsAnalyzed`
- **Definition**: If assignment is based on a unit other than participants, a description of the unit of assignment (for example, eyes, lesions, implants).
- **DataType**: Text
- **Limit**: 40 characters.





## Miscellaneous
## Submission tracking

### sub_tracking_estimated_results_date
- **Index Field:** `derivedSection.miscInfoModule.submissionTracking.estimatedResultsFirstSubmitDate`
- **Definition**: Results First Submitted Date but not yet Posted (e.g., still under QC review).
- **DataType**: NormalizedDate 


### version_holder
- **Index Field:** `derivedSection.miscInfoModule.versionHolder`
- **Definition**: The most recent date where Ingest ran successfully
- **DataType**: NormalizedDate 


### has_results
- **Index Field:** `hasResults`
- **Definition**: Flag that indicates if a study has posted results on public site
- **DataType**: Boolean (Yes/No)
