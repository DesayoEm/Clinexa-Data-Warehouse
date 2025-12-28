### Source: https://clinicaltrials.gov/data-api/about-api/study-data-structure


### EXCLUDED FIELDS

## Identification


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


## Organisation

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

## Design
### design_masking_desc
- **Index Field:** `protocolSection.designModule.designInfo.maskingInfo.maskingDescription`
- **Definition**: Information about other parties who may be masked in the clinical trial, if any.
- **DataType**: Markup
- **Limit**: 1000 characters
- **Reason**: Missing from most rows and has no analytical value


## Expanded access

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

## Eligibility

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


## status
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



## Large documents

### large_doc_no_sap
- **Index Field:** `documentSection.largeDocumentModule.noSap`
- **Definition**: Indication that No Statistical Analysis Plan (SAP) exists for this study.
- **DataType**: Boolean (Yes/No)
- **Reason**: No analytical value

## Miscellaneous

### unposted_responsible_party
- **Index Field:**  `annotationSection.annotationModule.unpostedAnnotation.unpostedResponsibleParty`
- **Definition**: Responsible Party for Unposted Events.
- **DataType**: text
- **Reason**: No analytical value


### sub_tracking_first_mcp_date
- **Index Field:** `derivedSection.miscInfoModule.submissionTracking.firstMcpInfo.postDateStruct.date`
- **Definition**: Date of first MCP posted date
- **DataType**: NormalizedDate
- **Reason**: Field not clearly described. can't determine analytical value


### sub_tracking_first_mcp_type
**Index Field:** derivedSection.miscInfoModule.submissionTracking.firstMcpInfo.postDateStruct.type

**Definition**: Date type for first MCP posted date
**DataType**: Enum(DateType) 
**Source Values**:

* ACTUAL - Actual
* ESTIMATED - Estimated

- **Reason**: Field not clearly described. can't determine analytical value


## contactsLocationsModule

### overall_officials
- **Index Field:**  `protocolSection.contactsLocationsModule.overallOfficials`
- **Definition**:  Person(s) responsible for the overall scientific leadership of the protocol, including study principal investigator.
- **DataType**: Official[]
- **Reason**: Officials lack contact information and serve administrative/ oversight purposes only. patient matching relies on location contacts, not officials


