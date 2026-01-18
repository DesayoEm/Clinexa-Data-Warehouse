
### Source: https://clinicaltrials.gov/data-api/about-api/study-data-structure

## Identification

### nct_id
- **Index Field:** `protocolSection.identificationModule.nctId`
- **Definition**: The unique identification code given to each clinical study upon registration at ClinicalTrials.gov. 
      The format is "NCT" followed by an 8-digit number.  Also known as ClinicalTrials.gov Identifier

### brief_title
- **Index Field:** `protocolSection.identificationModule.briefTitle`
- **Definition**: A short title of the clinical study written in language intended for the lay public.
- **Data Type**: Text
- **Limit**: 300 characters.


### official_title
- **Index Field:** `protocolSection.identificationModule.officialTitle`
- **Definition**:  The title of the clinical study, corresponding to the title of the protocol.
- **Data Type**: Text
- **Limit**: 600 characters

### acronym
- **Index Field:** `protocolSection.identificationModule.acronym`
- **Definition**: An acronym or abbreviation used publicly to identify the clinical study, if any.
- **Data Type**: Text
- **Limit**: 14 characters.


### org_study_id
- **Index Field:** `protocolSection.identificationModule.orgStudyIdInfo.id`
- **Definition**: Organization's Unique Protocol Identification Number
- **Data Type**: Text
- **Data Type**: Text
- **Limit**: 30 characters.

## Description

### brief_summary
- **Index Field:** `protocolSection.descriptionModule.briefSummary`
- **Definition**: A short description of the clinical study, including a brief statement of the clinical study's hypothesis, written in language intended for the lay public.
- **Limit**: 5,000  characters.


### detailed_desc
- **Index Field:** `protocolSection.descriptionModule.detailedDescription`
- **Definition**: Extended description of the protocol, including more technical information (as compared to the Brief Summary), if desired. Does not include the entire protocol
- **Limit**: 32,000  characters.


## Sponsor 
### responsible_party
- **Index Field:** `protocolSection.sponsorCollaboratorsModule.responsibleParty`
- **Definition**: An indication of whether the responsible party is the sponsor, the sponsor-investigator, or a principal investigator designated by the sponsor to be the responsible party
- **DataType**: Dict{Enum(ResponsiblePartyType)} # single value

**Source Values**: 
- * SPONSOR - Sponsor
- * PRINCIPAL_INVESTIGATOR - Principal Investigator
- * SPONSOR_INVESTIGATOR - Sponsor-Investigator

    

## Design

### study_type
- **Index Field:** `protocolSection.designModule.studyType`
- **Definition**: Study type
- **DataType**: Enum(StudyType)

**Source Values**: 
- * EXPANDED_ACCESS - Expanded Access
- * INTERVENTIONAL - Interventional
- * OBSERVATIONAL - Observational


### patient_registry
- **Index Field:** `protocolSection.designModule.patientRegistry`
- **Definition**: A type of observational study that collects information about patients' medical conditions and/or treatments to better understand how a condition or treatment affects patients in the real world.
- **DataType**: Boolean (True/False)

### enrollment_type
- **Index Field:** `protocolSection.designModule.enrollmentInfo.type`
- *Definition**: Enrollment type
- **DataType**: Enum(EnrollmentType)
**Source Values**: 
- * ACTUAL - Actual
- * ESTIMATED - Estimated


### enrollment_count
- **Index Field:** `protocolSection.designModule.enrollmentInfo.count`
- **Definition**: The estimated total number of participants to be enrolled (target number) or the actual total number of participants that are enrolled in the clinical study.
- **DataType**: Integer


### design_allocation
- **Index Field:** `protocolSection.designModule.designInfo.allocation`
- **Definition**: The method by which participants are assigned to arms in a clinical trial.
- **DataType**: Enum(DesignAllocation)

**Source Values**: 
- * RANDOMIZED - Randomized
- * NON_RANDOMIZED - Non-Randomized
- * NA - N/A


### design_intervention_model
- **Index Field:** `protocolSection.designModule.designInfo.interventionModel`
- **Definition**: The strategy for assigning interventions to participants.
- **DataType**: Enum(InterventionalAssignment)
**Source Values**:
- * SINGLE_GROUP - Single Group Assignment
- * PARALLEL - Parallel Assignment
- * CROSSOVER - Crossover Assignment
- * FACTORIAL - Factorial Assignment
- * SEQUENTIAL - Sequential Assignment


### design_intervention_model_desc`
- **Index Field:** `protocolSection.designModule.designInfo.interventionModelDescription`    
- **Definition**: details about the Interventional Study Model.
- **Limit**: 1,000 characters
- **DataType**: Markup


### design_primary_purpose
- **Index Field:** `protocolSection.designModule.designInfo.primaryPurpose`  
- **Definition**: The main objective of the intervention(s) being evaluated by the clinical trial
- **Limit**: 1,000 characters
- **DataType**: Enum(PrimaryPurpose)

**Source Values**:
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


### design_observational_model
- **Index Field:** `protocolSection.designModule.designInfo.observationalModel`
- **Definition**: Primary strategy for participant identification and follow-up.
- **DataType**: Enum(ObservationalModel)

**Source Values**:
-  COHORT - Cohort
- * CASE_CONTROL - Case-Control
- * CASE_ONLY - Case-Only
- * CASE_CROSSOVER - Case-Crossover
- * ECOLOGIC_OR_COMMUNITY - Ecologic or Community
- * FAMILY_BASED - Family-Based
- * DEFINED_POPULATION - Defined Population
- * NATURAL_HISTORY - Natural History
- * OTHER - Other



### design_time_perspective
- **Index Field:** `protocolSection.designModule.designInfo.timePerspective`
- **Definition**: Temporal relationship of observation period to time of participant enrollment..
- **DataType**: Enum(DesignTimePerspective)

**Source Values**:
- * RETROSPECTIVE - Retrospective
- * PROSPECTIVE - Prospective
- * CROSS_SECTIONAL - Cross-Sectional
- * OTHER - Other

### design_masking
- **Index Field:** `protocolSection.designModule.designInfo.maskingInfo.masking`
- **Definition**: The party or parties involved in the clinical trial who are prevented from having knowledge of the interventions assigned to individual participants
- **DataType**: Enum(DesignMasking)

**Source Values**:
- * NONE - None (Open Label)
- * SINGLE - Single
- * DOUBLE - Double
- * TRIPLE - Triple
- * QUADRUPLE - Quadruple

    


## Biospecimen

### biospec_retention
- **Index Field:** `protocolSection.designModule.bioSpec.retention`
- **Definition**:Whether samples of material from research participants are retained in a biorepository
- **DataType**: Enum(BioSpecRetention)
**Source Values**:
- * NONE_RETAINED - None Retained
- * SAMPLES_WITH_DNA - Samples With DNA
- * SAMPLES_WITHOUT_DNA - Samples Without DNA


### biospec_desc
- **Index Field:** `protocolSection.designModule.bioSpec.description`
- **Definition**:Specify all types of biospecimens to be retained (e.g., whole blood, serum, white cells, urine, tissue).
- **Limit**: 1,000 characters.



## Eligibility

### eligibility_criteria
**Index Field:** `protocolSection.eligibilityModule.eligibilityCriteria`

**Definition**: 	
A limited list of criteria for selection of participants in the clinical study, provided in terms of inclusion and exclusion criteria and suitable for assisting potential participants in identifying clinical studies of interest. 
Bulleted list for each criterion below the headers "Inclusion Criteria" and "Exclusion Criteria".

**Limit**: 20,000 characters.


### healthy_volunteers
- **Index Field:** `protocolSection.eligibilityModule.eligibilityCriteria`
- **Definition**: Indication that participants who do not have a disease or condition, or related conditions or symptoms, under study in the clinical study are permitted to participate in the clinical study.
**Optional for Observational Studies**
- **DataType**: boolean (Yes/No)

### sex
- **Index Field:** `protocolSection.eligibilityModule.sex`
- **Definition**: The sex of the participants eligible to participate in the clinical study
- **DataType**: Enum(Sex)

**Source Values**:
- * FEMALE - Female
- * MALE - Male
- * ALL - All

    
### min_age
- **Index Field:** `protocolSection.eligibilityModule.minimumAge`
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

### max_age
- **Index Field:** `protocolSection.eligibilityModule.maximumAge`
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



### population_desc
- **Index Field:** `protocolSection.eligibilityModule.studyPopulation`
- **Definition**: A description of the population from which the groups or cohorts will be selected
(for example, primary care clinic, community sample, residents of a certain town).
**For observational studies only**
- **DataType**: Markup
- *Limit**: 1,000 characters.


### sampling_method
- **Index Field:** `protocolSection.eligibilityModule.samplingMethod`
- **Definition**: The method used for the sampling approach

**For observational studies only**
- **DataType**: Enum (SamplingMethod)
**Source Values**:
- * PROBABILITY_SAMPLE - Probability Sample
- * NON_PROBABILITY_SAMPLE - Non-Probability Sample


## Status

### has_expanded_access
- **Index Field:** `protocolSection.statusModule.expandedAccessInfo.hasExpandedAcces`
- **Definition**:  Whether there is expanded access to the investigational product for patients who do not qualify for enrollment in a clinical trial.
- **DataType**: Boolean 

### expanded_access_nct
- **Index Field:** `protocolSection.statusModule.expandedAccessInfo.nctId`
- **Definition**:  If expanded access is available, the NCT number of the expanded access record.
- **DataType**: Boolean 


### expanded_access_status
- **Index Field:** `protocolSection.statusModule.expandedAccessInfo.statusForNctId`
- **Definition**: Recruitment status of the EA study that's linked to INT/OBS
- **DataType**: Boolean 


### overall_status
- **Index Field:** `protocolSection.statusModule.overallStatus`
- **Definition**: The recruitment status for the clinical study as a whole, based upon the status of the individual sites.
   If at least one facility in a multi-site clinical study has an Individual Site Status of "Recruiting," then the Overall Recruitment Status for the study must be "Recruiting."

- **DataType**: Enum (Status)
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


### last_known_status
- **Index Field:** `protocolSection.statusModule.lastKnownStatus`
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

### status_verified_date
- **Index Field:** `protocolSection.statusModule.statusVerifiedDate`
- **Definition**: The date on which the responsible party last verified the clinical study information in the entire ClinicalTrials.gov record for the clinical study, even if no additional or updated information is being submitted.
- **DataType**: PartialDate 


### start_date
- **Index Field:** `protocolSection.statusModule.startDateStruct.date`
- **Definition**: The estimated date on which the clinical study will be open for recruitment of participants, or the actual date on which the first participant was enrolled.
- **DataType**: PartialDate 


### start_date_type
- **Index Field:** `protocolSection.statusModule.startDateStruct.type`
- **Definition**: A study on ClinicalTrials.gov whose last known status was recruiting; not yet recruiting; or active, not recruiting but that has passed its completion date, and the status has not been last verified within the past 2 years.
- **DataType**: Enum (DateType)
**Source Values**:
- * ACTUAL - Actual
- * ESTIMATED - Estimated


### first_submit_date
- **Index Field:** `protocolSection.statusModule.studyFirstSubmitDate`
- **Definition**: The date on which the study sponsor or investigator first submitted a study record to ClinicalTrials.gov. There is typically a delay of a few days between the first submitted date and the record's availability on ClinicalTrials.gov (the first posted date).
- **DataType**: NormalizedDate  
    



### last_update_submit_date
- **Index Field:** `protocolSection.statusModule.lastUpdateSubmitDate`
- **Definition**: The most recent date on which the study sponsor or investigator submitted changes to a study record to ClinicalTrials.gov. There is typically a delay of a few days between the last update submitted date and when the date changes are posted on ClinicalTrials.gov (the last update posted date).
It is the responsibility of the sponsor or investigator to ensure that the study record is consistent with the NLM QC review criteria.
- **DataType**: NormalizedDate  


### completion_date
- **Index Field:** `protocolSection.statusModule.completionDateStruct.date`
- **Definition**: The date the final participant was examined or received an intervention for purposes of final collection of data for the primary and secondary outcome measures and adverse events (for example, last participant’s last visit), whether the clinical study concluded according to the pre-specified protocol or was terminated.
- **DataType**: PartialDate  


### completion_date_type
- **Index Field:** `protocolSection.statusModule.completionDateStruct.type`
- **Definition**: A study on ClinicalTrials.gov whose last known status was recruiting; not yet recruiting; or active, not recruiting but that has passed its completion date, and the status has not been last verified within the past 2 years.
- **DataType**: Enum (DateType)
**Source Values**:
- * ACTUAL - Actual
- * ESTIMATED - Estimated


### why_stopped
- **Index Field:** `protocolSection.statusModule.whyStopped`
- **Definition**: A brief explanation of the reason(s) why such clinical study was stopped (for a clinical study that is "Suspended," "Terminated," or "Withdrawn" prior to its planned completion as anticipated by the protocol).
- **DataType**: Markup
- **Limit**: 250 characters


## Oversight

### has_dmc
- **Index Field:** `protocolSection.oversightModule.oversightHasDmc`
- **Definition**: Whether a data monitoring committee has been appointed for this study.
    The data monitoring committee (board) is a group of independent scientists who are appointed to monitor the safety and scientific integrity of a human research intervention, and to make recommendations to the sponsor regarding the stopping of the trial for efficacy, for harms or for futility.
- **DataType**: Boolean (Yes/No)


### is_fda_regulated_drug
- **Index Field:** `protocolSection.oversightModule.isFdaRegulatedDrug`
- **Definition**: Whether a clinical study is studying a drug product (including a biological product) subject to section 505 of the Federal Food, Drug, and Cosmetic Act or to section 351 of the Public Health Service Act.
- **DataType**: Boolean (Yes/No)
**Optional for Observational Studies**


### is_fda_regulated_device
- **Index Field:** `protocolSection.oversightModule.isFdaRegulatedDevice`
- **Definition**: Whether clinical study is studying a device product subject to section 510(k), 515, or 520(m) of the Federal Food, Drug, and Cosmetic Act.
- **DataType**: Boolean (Yes/No)
**Optional for Observational Studies**


### is_unapproved_device
- **Index Field:** `protocolSection.oversightModule.isUnapprovedDevice`
- **Definition**: Indication that at least one device product studied in the clinical study has not been previously approved or cleared by the U.S. Food and Drug Administration (FDA) for one or more uses.
- **DataType**: Boolean (true/false)
- **Field is asserted only when true; absence does not imply false.**


### is_us_export
- **Index Field:** `protocolSection.oversightModule.isUsExport`
- **Definition**: Whether any drug product (including a biological product) or device product studied in the clinical study is manufactured in the United States or one of its territories and exported for study in a clinical study in another country.
Required if U.S. FDA-regulated Drug and/or U.S. FDA-regulated Device is "Yes," U.S. FDA IND or IDE is "No", and Facility Information does not include at least one U.S. location.
- **DataType**: Boolean (Yes/No)



## Individual participant data

### ipd_sharing
- **Index Field:** `protocolSection.ipdSharingStatementModule.ipdSharing`
- **Definition**: Indication  whether there is a plan to make individual participant data (IPD) collected in this study, including data dictionaries, available to other researchers (typically after the end of the study).
- **DataType**: Enum (DateType)
**Source Values**:
- * YES - Yes
- * NO - No
- * UNDECIDED - Undecided


### ipd_desc
- **Index Field:** `protocolSection.ipdSharingStatementModule.description`
- **Definition**: What specific individual participant data sets are to be shared (for example, all collected IPD, all IPD that underlie results in a publication).
If the Plan to Share IPD is "No" or "Undecided," an explanation may be provided for why IPD will not be shared or why it is not yet decided.
- **DataType**: Markup
- **Limit**: 1,000 characters.


### ipd_time_frame
- **Index Field:** `protocolSection.ipdSharingStatementModule.timeFrame`
- **Definition**: A description of when the IPD and any additional supporting information will become available and for how long, including the start and end dates or period of availability.
This may be provided as an absolute date (for example, starting in January 2025) or as a date relative to the time when summary data are published or otherwise made available (for example, starting 6 months after publication).
- **DataType**: Markup
- **Limit**: 1,000 characters.


### ipd_access_criteria
- **Index Field:** `protocolSection.ipdSharingStatementModule.accessCriteria`
- **Definition**: Describe by what access criteria IPD and any additional supporting information will be shared, including with whom, for what types of analyses, and by what mechanism.
Information about who will review requests and criteria for reviewing requests may also be provided.
- **DataType**: Markup
- **Limit**: 1,000 characters.


### ipd_url
- **Index Field:** `protocolSection.ipdSharingStatementModule.url`   
- **Definition**: The web address, if any, used to find additional information about the plan to share IPD.
- **DataType**: Text
- **Limit**: 3,999 characters.



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


## Point of contact

### poc_title
- **Index Field:** `resultsSection.moreInfoModule.pointOfContact.title`
- **Definition**: The person who is designated the point of contact.
This may be a specific person's name (for example, Dr. Jane Smith) or a position title (for example, Director of Clinical Trials).
- **DataType**: Text


### poc_organization
- **Index Field:** `resultsSection.moreInfoModule.pointOfContact.organization`
- **Definition**: Full name of the designated individual's organizational affiliation.
- **DataType**: Text


### poc_email
- **Index Field:** `resultsSection.moreInfoModule.pointOfContact.email`
- **Definition**: Electronic mail address of the designated individual.
- **DataType**: Text


### poc_phone
- **Index Field:** `resultsSection.moreInfoModule.pointOfContact.phone`
- **Definition**: Office phone number of the designated individual. Format 123-456-7890 within the United States and Canada. 

If outside the United States and Canada, the full phone number, including the country code is provided.

**DataType**: Text


### poc_phone_ext
- **Index Field:** `resultsSection.moreInfoModule.pointOfContact.phoneExt`
- **Definition**: Phone extension, if needed


## Submission tracking

### sub_tracking_estimated_results_date
- **Index Field:** `derivedSection.miscInfoModule.submissionTracking.estimatedResultsFirstSubmitDate`
- **Definition**: Results First Submitted Date but not yet Posted (e.g., still under QC review).
- **DataType**: NormalizedDate 



### last_updated
- **Index Field:** `protocolSection.statusModule.lastUpdatePostDateStruct.date`
- **Definition**: The most recent date on which changes to a study record were made available on ClinicalTrials.gov.
There may be a delay between when the changes were submitted to ClinicalTrials.gov by the study's sponsor or investigator (the last update submitted date) and the last update posted date.
- **DataType**: NormalizedDate 


### limitations_desc
- **Index Field:** `resultsSection.moreInfoModule.limitationsAndCaveats.description`
- **Definition**: Significant limitations of the study. 
Such limitations may include not reaching the target number of participants needed to achieve target power and statistically reliable results or technical problems with measurements leading to unreliable or uninterpretable data.
- **DataType**: Markup
- **Limit**: 500 characters.


## Certain agreements

### certain_agreement_pi_sponsor_employee
- **Index Field:** `resultsSection.moreInfoModule.certainAgreement.piSponsorEmployee`
- **Definition**: Whether the principal investigator is an employee of the sponsor.
- **DataType**: Boolean (Yes/ No)
- **Limit**: 500 characters.


### certain_agreement_restrictive
- **Index Field:** `resultsSection.moreInfoModule.certainAgreement.restrictiveAgreement`
- **Definition**: Whether there exists any agreement (other than an agreement solely to comply with applicable provisions of law protecting the privacy of participants participating in the clinical study) between the sponsor or its agent and the principal investigator (PI) that restricts in any manner the ability of the PI to discuss the results of the clinical study at a scientific meeting or any other public or private forum or to publish in a scientific or academic journal the results of the clinical study, after the
- **DataType**: Boolean (Yes/ No)
- **Limit**: 500 characters.


### certain_agreement_restriction_type
- **Index Field:** `resultsSection.moreInfoModule.certainAgreement.restrictionType`
- **Definition**: Additional information about the results disclosure restriction. 

The type that represents the most restrictive of the agreements is used If there are varying agreements
- **DataType**: Enum (AgreementRestrictionType)
**Source Values**:

- * LTE60 - LTE60
- * GT60 - GT60
- * OTHER - OTHER


### certain_agreement_other_details
- **Index Field:** `resultsSection.moreInfoModule.certainAgreement.otherDetails`

- **Definition**: The type of agreement including any provisions allowing the sponsor to require changes, ban the communication, or extend an embargo if "Other" disclosure agreement is selected on `resultsSection.moreInfoModule.certainAgreement.restrictionType`
- **DataType**: Markup
**Limit**: 500 characters.


## Miscellaneous

### version_holder
- **Index Field:** `derivedSection.miscInfoModule.versionHolder`
- **Definition**: The most recent date where Ingest ran successfully
- **DataType**: NormalizedDate 


### has_results
- **Index Field:** `hasResults`
- **Definition**: Flag that indicates if a study has posted results on public site
- **DataType**: Boolean (Yes/No)

