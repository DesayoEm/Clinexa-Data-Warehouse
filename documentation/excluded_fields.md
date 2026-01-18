
# EXCLUDED FIELDS

## identificationModule

### org_study_type
- **Index Field:** `protocolSection.identificationModule.orgStudyIdInfo.type`
- **Definition**: Type of organization's unique protocol ID
- **DataType**: Enum(OrgStudyIdType)
- **Reason**: Missing from most rows and has no signal value


### org_study_link
- **Index Field:** `protocolSection.identificationModule.orgStudyIdInfo.link`
- **Definition**: URL link based on OrgStudyId and OrgStudyIdType input in PRS, include system-generated links to NIH RePORTER, specifically (associated with the types of federal funding identified as OrgStudyIdType)
- **DataType**: Enum(OrgStudyIdType)
- **Reason**: Missing from most rows and has no analytical value


### org_name
- **Index Field:** `protocolSection.identificationModule.organization.fullName`
- **Definition**: A (registered) organization (typically the Responsible Party) that sponsors the clinical  trial (study)
- **Limit**: 5,000  characters.
- **Reason**: Information provided in `protocolSection.sponsorCollaboratorsModule`
- 
### org_class
- **Index Field:** `protocolSection.identificationModule.organization.class`
- **Definition**: Organization type
- **DataType**: Dict{Enum(AgencyClass)} 

**Source Values**: 
- * NIH - NIH
- * FED - FED
- * OTHER_GOV - OTHER_GOV
- * INDIV - INDIV
- * INDUSTRY - INDUSTRY
- * NETWORK - NETWORK
- * AMBIG - AMBIG
- * OTHER - OTHER
- * UNKNOWN - UNKNOWN

- **Reason**: Information provided in `protocolSection.sponsorCollaboratorsModule`

---

## designModule

### design_masking_desc
- **Index Field:** `protocolSection.designModule.designInfo.maskingInfo.maskingDescription`
- **Definition**: Information about other parties who may be masked in the clinical trial, if any.
- **DataType**: Markup
- **Limit**: 1000 characters
- **Reason**: Missing from most rows and has no analytical value


### exp_acc_type_individual
- **Index Field:** `protocolSection.designModule.expandedAccessTypes.individual`
- **Definition**:For individual participants, including for emergency use, as specified in 21 CFR 312.310. 
     Allows a single patient, with a serious disease or condition who cannot participate in a clinical trial, access to a drug or biological product that has not been approved by the FDA. This category also includes access in an emergency situation.
     This type of expanded access is used when multiple patients with the same disease or condition seek access to a specific drug or biological product that has not been approved by the FDA.
- **DataType**: boolean (Yes/No)
- **Reason**: Not present in raw API payload; UI-only construct


### exp_acc_type_intermediate
- **Index Field:** `protocolSection.designModule.expandedAccessTypes.intermediate`
- **Definition**:For intermediate-size participant populations, as specified in 21 CFR 312.315. 
     Allows more than one patient (but generally fewer patients than through a Treatment IND/Protocol) access to a drug or biological product that has not been approved by the FDA.
     This type of expanded access is used when multiple patients with the same disease or condition seek access to a specific drug or biological product that has not been approved by the FDA.
- **DataType**: boolean (Yes/No)
- **Reason**: Not present in raw API payload; UI-only construct


### exp_acc_type_treatment
- **Index Field:** `protocolSection.designModule.expandedAccessTypes.individual`
- **Definition**:,For intermediate-size participant populations, as specified in 21 CFR 312.315.
   Allows more than one patient (but generally fewer patients than through a Treatment IND/Protocol) access to a drug or biological product that has not been approved by the FDA.
   This type of expanded access is used when multiple patients with the same disease or condition seek access to a specific drug or biological product that has not been approved by the FDA
- **DataType**: boolean (Yes/No)
- **Reason**: Not present in raw API payload; UI-only construct

---


## eligibilityModule

### gender_based
- **Index Field:** `protocolSection.eligibilityModule.genderBased`
- **Definition**: Whether participant eligibility is based on gender
- **DataType**: boolean (Yes/No)
- **Reason**: Redundant AND missing in most rows. This information could be inferred from `protocolSection.eligibilityModule.sex`

### gender_desc
- **Index Field:** `protocolSection.eligibilityModule.genderDescription`
- **Definition**: Descriptive information about Gender criteria IF eligibility is based on gender
- **DataType**: Markup
- **Limit**: 1,000 characters.
- **Reason**: Redundant AND missing in most rows.

### std_age
- **Index Field:** `protocolSection.eligibilityModule.stdAges`
- **Definition**: Ingest calculated the StdAge if there is minimumAge and/or maximimumAge entered. Redacted for Withheld studies
- **DataType**: [Enum(StandardAge)]

**Source Values**:
- * FEMALE - Female
* CHILD - Child
- * ADULT - Adult
- * OLDER_ADULT - Older Adult
- **Reason**: Data is presented as an array and information could be inferred from `protocolSection.eligibilityModule.maximumAge` and `protocolSection.eligibilityModule.minimumAge`

---

## armsInterventionsModule

##### `armGroupLabels`
- **Index Field:** `protocolSection.armsInterventionsModule.armGroups.armGroupLabels`
- **Definition**: Labels of arm groups that receive this intervention. References `armGroups[].label` within the same study.
- **Data Type**: Array of Text
- **Required**: Yes (if multiple arms exist)


### Arm <-->Intervention Relationship

**Source Data**: The API provides bidirectional references:
- `armGroups[].interventionNames` — intervention names per arm
- `interventions[].armGroupLabels` — arm labels per intervention

**Decision**: `armGroups[].interventionNames` as the source of truth for arm interventions and interventions[] as the source of truth interventions

**Rationale**:
1. Matches analytical workflow (arm -> intervention, not reverse)
2. User-entered data may have inconsistencies between the two lists
3. Avoids reconciliation logic and potential mismatches from bidirectional data quality issues

**Implication**: Queries for "which arms use this intervention" require joining through `bridge_arm_interventions` from the arm side. We do not model the reverse relationship from `interventions[].armGroupLabels`.



---

## statusModule
### delayed_posting
- **Index Field:** `protocolSection.statusModule.delayedPosting`
- **Definition**:Post Prior to U.S. FDA Approval or Clearance
- Authorize NIH to post publicly clinical trial registration information for a clinical study of a device product that has not been previously approved or cleared (that would otherwise be subject to delayed posting).
**DataType**: Boolean (Yes/No)
- **Reason**: Not present in raw API payload; UI-only construct



### first_submit_qc_date
- **Index Field:** `protocolSection.statusModule.studyFirstSubmitQcDate`
- **Definition**: The date on which the study sponsor or investigator first submits a study record that is consistent with National Library of Medicine (NLM) quality control (QC) review criteria. The sponsor or investigator may need to revise and submit a study record one or more times before NLM's QC review criteria are met.
It is the responsibility of the sponsor or investigator to ensure that the study record is consistent with the NLM QC review criteria.
- **DataType**: NormalizedDate  
- **Reason**: No analytical value

---


## contactsLocationsModule

### overall_officials
- **Index Field:**  `protocolSection.contactsLocationsModule.overallOfficials`
- **Definition**:  Person(s) responsible for the overall scientific leadership of the protocol, including study principal investigator.
- **DataType**: Official[]
- **Reason**: Officials lack contact information and serve administrative/ oversight purposes only. patient matching relies on location contacts, not officials

----

## referencesModule

### references
-**Index Field:** `protocolSection.referencesModule.references`
-**Definition**: Citations to publications related to the protocol
- **DataType**: Reference[]

#### Fields
##### `citations`
- **Description**: PubMed identifier for the citation in MEDLINE
- **Data Type**: Text
- **Required**: Yes
- **Reason**: No analytical value

##### `retractions`
- **Description**: retractions of the citation
- **Data Type**: Text
- **Required**: Yes
- **Reason**: No analytical value


---

### baselineMeasuresModule
- **Index Field**: `resultsSection.baselineMeasuresModule`
- **Description**: A table of demographic and baseline measures and data collected by arm or comparison group and for the entire population of participants in the clinical study.
- **Reason for Exclusion**: Contains metadata about what baseline demographics were collected (age, gender, etc.) but not the actual measured values. 
  No analytical value. Documentation/compliance data

---

### annotationModule
- **Index Field:** `annotationSection.annotationModule`
- **Content:** Administrative metadata tracking results submission/QA workflow
- **Event Types:** RELEASE (results submitted), UNRELEASE (withdrawn), RESET (pulled back for revision)

**Reason for Exclusion:**
The annotation module tracks ClinicalTrials.gov's internal submission and quality assurance workflow, not clinical trial data. It contains administrative events like when results were submitted, withdrawn, or reset during the review process.

**Example:**
```json
{
  "unpostedResponsibleParty": "CrossComm, Inc.",
  "unpostedEvents": [
    {"type": "RELEASE", "date": "2024-09-20"},
    {"type": "RESET", "date": "2024-10-14"}
  ]
}
```
---

### documentSection / largeDocModule

- **Index Field:** `documentSection`, `protocolSection.largeDocModule`
- **Content:** File attachments (protocols, consent forms, statistical analysis plans)
  - **Reason for Exclusion:** Links to documents, not structured data. Files would need separate download and parsing. 
            Study protocols and documents are not used in patient matching or R&D analytics models. 
---


### miscInfoModule
- **Index Field:** `derivedSection.miscInfoModule`
- **Content:** System metadata, removed countries, results submission workflow tracking

**Reason for Exclusion:**

**1. Version Holder**
- System metadata (last successful ClinicalTrials.gov data ingestion timestamp)
- Not clinical or analytical data
- No relevance to patient matching or R&D analytics

**2. Removed Countries**
- Historical record of countries where all study locations were closed/removed
- Already reflected in current study_locations table (active sites only)
- Patient matching uses current locations, not historical changes
- Weak analytical signal for R&D (doesn't predict outcomes)

**3. Submission Tracking**
- Detailed workflow metadata (release dates, reset dates, Major Comment Postings)
- Tracks ClinicalTrials.gov QA/review cycles
- Similar to annotationModule - administrative process, not clinical data
- Number of submission resets/MCPs is weak predictor of trial quality


