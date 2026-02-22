SCALAR_FIELDS = {
    # identificationModule
    "nct_id": "protocolSection.identificationModule.nctId",
    "brief_title": "protocolSection.identificationModule.briefTitle",
    "official_title": "protocolSection.identificationModule.officialTitle",
    "acronym": "protocolSection.identificationModule.acronym",
    "org_study_id": "protocolSection.identificationModule.orgStudyIdInfo.id",
    # descriptionModule
    "brief_summary": "protocolSection.descriptionModule.briefSummary",
    "detailed_desc": "protocolSection.descriptionModule.detailedDescription",
    # sponsorCollaboratorsModule
    "responsible_party_type": "protocolSection.sponsorCollaboratorsModule.responsibleParty.type",
    # Design
    "study_type": "protocolSection.designModule.studyType",
    "patient_registry": "protocolSection.designModule.patientRegistry",
    "enrollment_type": "protocolSection.designModule.enrollmentInfo.type",
    "enrollment_count": "protocolSection.designModule.enrollmentInfo.count",
    "design_allocation": "protocolSection.designModule.designInfo.allocation",
    "design_intervention_model": "protocolSection.designModule.designInfo.interventionModel",
    "design_intervention_model_desc": "protocolSection.designModule.designInfo.interventionModelDescription",
    "design_primary_purpose": "protocolSection.designModule.designInfo.primaryPurpose",
    "design_observational_model": "protocolSection.designModule.designInfo.observationalModel",
    "design_time_perspective": "protocolSection.designModule.designInfo.timePerspective",
    "design_masking": "protocolSection.designModule.designInfo.maskingInfo.masking",
    # Biospecimen
    "biospec_retention": "protocolSection.designModule.bioSpec.retention",
    "biospec_desc": "protocolSection.designModule.bioSpec.description",
    # Eligibility
    "eligibility_criteria": "protocolSection.eligibilityModule.eligibilityCriteria",
    "healthy_volunteers": "protocolSection.eligibilityModule.healthyVolunteers",
    "sex": "protocolSection.eligibilityModule.sex",
    "min_age_raw": "protocolSection.eligibilityModule.minimumAge",
    "max_age_raw": "protocolSection.eligibilityModule.maximumAge",
    "population_desc": "protocolSection.eligibilityModule.studyPopulation",
    "sampling_method": "protocolSection.eligibilityModule.samplingMethod",
    # Status
    "overall_status": "protocolSection.statusModule.overallStatus",
    "last_known_status": "protocolSection.statusModule.lastKnownStatus",
    "status_verified_date": "protocolSection.statusModule.statusVerifiedDate",
    "start_date": "protocolSection.statusModule.startDateStruct.date",
    "start_date_type": "protocolSection.statusModule.startDateStruct.type",
    "first_submit_date": "protocolSection.statusModule.studyFirstSubmitDate",
    "last_update_submit_date": "protocolSection.statusModule.lastUpdateSubmitDate",
    "completion_date": "protocolSection.statusModule.completionDateStruct.date",
    "completion_date_type": "protocolSection.statusModule.completionDateStruct.type",
    "why_stopped": "protocolSection.statusModule.whyStopped",
    "has_expanded_access": "protocolSection.statusModule.expandedAccessInfo.hasExpandedAccess",
    "expanded_access_nct": "protocolSection.statusModule.expandedAccessInfo.nctId",
    "expanded_access_status": "protocolSection.statusModule.expandedAccessInfo.statusForNctId",
    # Oversight
    "has_dmc": "protocolSection.oversightModule.oversightHasDmc",
    "is_fda_regulated_drug": "protocolSection.oversightModule.isFdaRegulatedDrug",
    "is_fda_regulated_device": "protocolSection.oversightModule.isFdaRegulatedDevice",
    "is_unapproved_device": "protocolSection.oversightModule.isUnapprovedDevice",
    "is_us_export": "protocolSection.oversightModule.isUsExport",
    # Individual participant data
    "ipd_sharing": "protocolSection.ipdSharingStatementModule.ipdSharing",
    "ipd_desc": "protocolSection.ipdSharingStatementModule.description",
    "ipd_time_frame": "protocolSection.ipdSharingStatementModule.timeFrame",
    "ipd_access_criteria": "protocolSection.ipdSharingStatementModule.accessCriteria",
    "ipd_url": "protocolSection.ipdSharingStatementModule.url",
    # contacts
    "poc_title": "resultsSection.moreInfoModule.pointOfContact.title",
    "poc_organization": "resultsSection.moreInfoModule.pointOfContact.organization",
    "poc_email": "resultsSection.moreInfoModule.pointOfContact.email",
    "poc_phone": "resultsSection.moreInfoModule.pointOfContact.phone",
    "poc_phone_ext": "resultsSection.moreInfoModule.pointOfContact.phoneExt",
    # Participant flow
    "flow_pre_assignment_details": "resultsSection.participantFlowModule.preAssignmentDetails",
    "flow_recruitment_details": "resultsSection.participantFlowModule.recruitmentDetails",
    "flow_type_units_analysed": "resultsSection.participantFlowModule.typeUnitsAnalyzed",
    # Certain agreements
    "certain_agreement_pi_sponsor_employee": "resultsSection.moreInfoModule.certainAgreement.piSponsorEmployee",
    "certain_agreement_restrictive": "resultsSection.moreInfoModule.certainAgreement.restrictiveAgreement",
    "certain_agreement_restriction_type": "resultsSection.moreInfoModule.certainAgreement.restrictionType",
    "certain_agreement_other_details": "resultsSection.moreInfoModule.certainAgreement.otherDetails",
    # Submission tracking
    "sub_tracking_estimated_results_date": "derivedSection.miscInfoModule.submissionTracking.estimatedResultsFirstSubmitDate",
    # Miscellaneous
    "version_holder": "derivedSection.miscInfoModule.versionHolder",
    "has_results": "hasResults",
    "last_updated": "protocolSection.statusModule.lastUpdatePostDateStruct.date",
    "limitations_desc": "resultsSection.moreInfoModule.limitationsAndCaveats.description",
}

NON_SCALAR_FIELDS = {
    # identificationModule
    "identification": {
        "index_field": "protocolSection.identificationModule",
        "non-scalar_fields": {
            "nctIdAliases": [],
            "SecondaryIdInfo": ["id", "type", "domain", "link"],
        },
    },
    # sponsorCollaboratorsModule
    "sponsor_collaborators": {  # SCALAR BUT TREATED AS A SEPARATE DIM
        "index_field": "protocolSection.sponsorCollaboratorsModule",
        "non-scalar_fields": {
            "sponsors": ["name", "class"],
            "collaborators": ["name", "class"],
        },
    },
    # conditionsModule
    "conditions": {
        "index_field": "protocolSection.conditionsModule",
        "non-scalar_fields": {
            "conditions": ["name", "class"],
            "keywords": ["name", "class"],
        },
    },
    # armsInterventions
    "arms_interventions": {
        "index_field": "protocolSection.armsInterventionsModule",
        "non-scalar_fields": {
            "armGroups": ["label", "type", "description"],
            "interventions": ["type", "name", "description"],
        },
    },
    # outcomesModule
    "outcomes": {
        "index_field": "protocolSection.outcomesModule",
        "non-scalar_fields": {
            "primaryOutcomes": ["measure", "description", "timeFrame"],
            "secondaryOutcomes": ["measure", "description", "timeFrame"],
            "otherOutcomes": ["measure", "description", "timeFrame"],
        },
    },
    # contactsLocationsModule
    "contacts_location": {
        "index_field": "protocolSection.contactsLocationsModule",
        "non-scalar_fields": {
            "central_contacts": ["name", "role", "email", "phone", "phoneExt"],
            "locations": [
                ["facility", "city", "state", "zip", "country", "status"],
                {"geoPoint": ["lat", "lon"]},
                {"contacts": ["name", "role", "email", "phone", "phoneExt"]},
            ],
        },
    },
    # referencesModule
    "references": {
        "index_field": "protocolSection.referencesModule",
        "non-scalar_fields": {
            "references": ["pmid", "type"],
            "see_also": ["label", "url"],
            "avail_ipds": ["id", "type", "url", "comment"],
        },
    },
    # outcomeMeasuresModule
    "outcome_measures": {
        "index_field": "resultsSection.outcomeMeasuresModule",
        "non-scalar_fields": {
            "outcomeMeasures": [
                "type",
                "title",
                "description",
                "populationDescription",
                "reportingStatus",
                "anticipatedPostingDate",
                "paramType",
                "dispersionType",
                "unitOfMeasure",
                "calculatePct",
                "timeFrame",
                "denomUnitsSelected" "typeUnitsAnalyzed",
            ],
            "OutcomeGroup": ["id", "title", "description"],
            "OutcomeDenom": ["units", "counts", ["groupId", "value"]],
            "OutcomeClass": ["categories", "comment", "achievements"],
            "OutcomeAnalysis ": ["type", "comment", "reasons"],
        },
    },
    # participantFlowModule
    "participant_flow": {
        "index_field": "resultsSection.participantFlowModule",
        "non-scalar_fields": {
            "groups": ["id", "title", "description"],
            "periods": [
                {"milestones": ["type", "comment", "achievements"]},
                {"dropWithdraws": ["type", "comment", "reasons"]},
            ],
        },
    },
    # adverseEventsModule
    "adverse_events": {
        "index_field": "resultsSection.adverseEventsModule",
        "non-scalar_fields": {
            "eventGroups": [
                "id",
                "title",
                "description",
                "deathsNumAffected",
                "deathsNumAffected",
                "deathsNumAtRisk",
                "seriousNumAffected",
                "seriousNumAtRisk",
                "otherNumAffected",
                "otherNumAtRisk",
            ],
            "seriousEvents": [
                "term",
                "organSystem",
                "sourceVocabulary",
                "notes",
                ["groupId", "numEvents", "numAffected", "numAtRisk"],
            ],
            "OtherEvent ": [
                "term",
                "organSystem",
                "sourceVocabulary",
                "notes",
                ["groupId", "numEvents", "numAffected", "numAtRisk"],
            ],
        },
    },
    # annotationModule
    "annotations": {
        "index_field": "annotationSection.annotationModule.violationAnnotation.violationEvents",
        "non-scalar_fields": {
            "violations": [
                "type",
                "description",
                "creationDate",
                "issuedDate",
                "releaseDate",
                "postedDate",
            ],
        },
    },
    # conditionBrowseModule
    "conditions_browse": {
        "index_field": "derivedSection.conditionBrowseModule",
        "non-scalar_fields": {
            "meshes": ["id", "term"],
            "ancestors": ["id", "term"],
            "browseLeaves": ["id", "name", "asFound", "relevance"],
            "browseBranches": ["abbrev", "name"],
        },
    },
    # interventionBrowseModule
    "interventions_browse": {
        "index_field": "derivedSection.interventionBrowseModule",
        "non-scalar_fields": {
            "meshes": ["id", "term"],
            "ancestors": ["id", "term"],
            "browseLeaves": ["id", "name", "asFound", "relevance"],
            "browseBranches": ["abbrev", "name"],
        },
    },
}
