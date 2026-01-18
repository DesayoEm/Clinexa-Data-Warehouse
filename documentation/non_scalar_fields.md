
### Source: https://clinicaltrials.gov/data-api/about-api/study-data-structure


## identificationModule

- **Index Field:** `protocolSection.identificationModule`
- **Description**: Study Identification

### Non-scalar fields
##### `nctIdAliases`
- **Definition**: Identifier(s) that are considered "Obsolete" or "Duplicate".
- **Data Type**: nct[]


##### `SecondaryIdInfo`
- **Definition**: An identifier(s) (ID), if any, other than the organization's Unique Protocol Identification Number or the NCT number that is assigned to the clinical study.
- **Data Type**: SecondaryIdInfo[]
- **Fields**: [id, type, domain, link]


---

## SponsorCollaboratorsModule

- **Index Field:** `protocolSection.sponsorCollaboratorsModule`
- **Description**: Organizations responsible for the study.


### Non-scalar fields
### `sponsor`

- **Definition**: Name of the sponsoring entity or individual
- **Data Type**: Sponsor
- **Fields**: [name,class]


### `collaborators`

- **Definition**: Other organizations, if any, providing support. Support may include funding, design, implementation, data analysis or reporting.
- **Data Type**: Sponsor[]
- **Fields**: [name,class]


- **Lead Sponsor**: Exactly 1 per study. The organization or person who initiates the study and has authority and control over it.
- **Collaborators**: 0 to many. Other organizations providing support (funding, design, implementation, data analysis, reporting).

---


## ConditionsModule 

- **Index Field:** `protocolSection.conditionsModule`
- **Description**: The name(s) of the disease(s) or condition(s) studied in the clinical study, or the focus of the clinical study.

### Non-scalar fields
###  `conditions`

- **Object Type**: text[]

### `keywords`

- **Description**: Words or phrases that best describe the protocol. Keywords help users find studies in the database
- **Object Type**: text[]

---

## armsInterventionsModule 

- **Index Field:** `protocolSection.armsInterventionsModule`
- **Description**: A description of each arm of the clinical trial that indicates its role in the clinical trial

### Non-scalar fields
### `ArmGroup`

- **Object Type**: ArmGroup[]
- **Description**: Pre-specified group or subgroup of participants assigned to receive specific intervention(s) (or no intervention) according to protocol. For interventional studies only. Observational studies use Groups/Cohorts with the same structure but different semantics.
- **Fields**: [label,type, description, [interventionNames]]


### `intervention`

- **Description**: The intervention(s) studied in the clinical trial. For interventional studies, at least one required. For observational studies, specifies interventions/exposures of interest if any.
- **Object Type**: Intervention[]
- **Fields**: [name,type, description, [otherNames]]


### Arm <-->Intervention Relationship

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

## SponsorCollaboratorsModule

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

## contactsLocationsModule

- **Index Field:** `protocolSection.contactsLocationsModule`
- **Description**:Contacts, Locations, and Investigator Information

### Non-scalar fields

### `centralContacts`
- **Definition**: Contact person(s) for general enrollment questions across all study locations. Required if no facility-level contacts provided.
- **Object Type**: centralContacts[]
- **Cardinality**: 0 to many (but at least one central OR facility contact required per study)
- **Fields**: [name,role, phone, phoneExt, email]


### `locations`

- **Object Type**: Location[]
- **Description**: Participating facility in a clinical study
- **Fields**: [facility,status, city, state, zip, geoPoint:[lat,lon], contacts:{}]

NOTE: Contacts are stored denormalized as JSON since not used for filtering/analysis.

---

## referencesModule

- **Index Field:** `protocolSection.referencesModule`
- **Description**:Citations to publications related to the protocol

### Non-scalar fields

### `references`
- **Definition**: Citations to publications related to the protocol
- **DataType**: Reference[]
- **Fields**: [pmid, type, citations]


### `see_also`
- **Definition**:  A website directly relevant to the protocol 
- **DataType**: SeeAlsoLink[]
- **Fields**: [label, url]


### availIpds
- **Definition**: Available individual participant data (IPD) sets and supporting information that are being shared for the study.
- **DataType**: AvailIpd[]
- **Fields**: [id, type, url, comment]


---


## Outcome measures module

- **Index Field:** `resultsSection.outcomeMeasuresModule`
- **Definition**: Outcome measures
- **DataType**: OutcomeMeasuresModule

### Non-scalar fields

### `outcomeMeasures`
- **Definition**: "Outcome measure" means a pre-specified measurement that is used to determine the effect of an experimental variable on participants in the study. 
- **DataType**: OutcomeMeasure[]
- **Fields**: [title, description, populationDescription, reportingStatus, anticipatedPostingDate,paramType
              dispersionType, unitOfMeasure, calculatePct, timeFrame, typeUnitsAnalyzed, denomUnitsSelected
               ]


### `groups`
- **Definition**:  Arms or comparison groups in the study,
- **DataType**: OutcomeGroup[]
- **Fields**: [id, title, description]
- 

### `denoms`
- **Definition**:  Analysis units and counts
- **DataType**: Denom[]
- **Fields**: [units, [counts]]

### `classes`
- **Definition**:  Arms or comparison groups in the study, 
- including all arms or comparison groups based on the pre-specified protocol and/or statistical analysis plan.
- **DataType**: MeasureClass[]
- **Fields**: [units, counts[groupId, value]]

---

## participantFlowModule

- **Index Field:** `resultsSection.participantFlowModule`
- **Definition**: Information to document the progress of research participants through each stage of a study 
- **DataType**: participantFlowModule



### Non-scalar fields

### `groups`
- **Definition**: Arms or groups for describing the flow of participants through the clinical study.
- **DataType**: FlowGroup[]
- **Fields**: [id, title, description]


### `periods`
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

## adverseEventsModule

- **Index Field:** `resultsSection.adverseEventsModule`
- **Definition**: Information for completing three tables summarizing adverse events.
- **DataType**: AdverseEventsModule


### Non-scalar fields

### `eventGroups`
- **Definition**: Arms or comparison groups in the study,
- **DataType**: EventGroup[] 
- **Fields**: [id, title, description, deathsNumAtRisk, seriousNumAffected, seriousNumAtRisk ,otherNumAffected]


### `seriousEvents`
- **Definition**: A table of all anticipated and unanticipated serious adverse events, grouped by organ system, with the number and frequency of such events by arm or comparison group of the clinical study.
- **DataType**: AdverseEvent[]
- **Fields**: [term, organSystem, sourceVocabulary, assessmentType, notes, stats ,otherNumAffected]


### `otherEvents`
- **Definition**:Other (Not Including Serious) Adverse Events - similar to Serious AE
- **DataType**: AdverseEvent[]
- **Fields**: [term, organSystem, sourceVocabulary, assessmentType, notes, stats ,otherNumAffected]

---



## annotationModule

### Non-scalar fields

### `violationEvents`
-**Index Field(s):** `annotationSection.annotationModule.violationAnnotation.violationEvents`
- **Description**: Violation EventS
- **DataType**: ViolationEvent[]
- **Fields**: [type, description, creationDate, issuedDate, 
                  releaseDate, postedDate
           ]

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