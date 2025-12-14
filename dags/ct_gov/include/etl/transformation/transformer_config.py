ONE_TO_ONE_FIELDS = {
    # Identification
    "nct_id": "protocolSection.identificationModule.nctId",
    "brief_title": "protocolSection.descriptionModule.briefSummary",
    "official_title": "protocolSection.identificationModule.officialTitle",
    "acronym": "protocolSection.identificationModule.acronym",
    "org_study_id": "protocolSection.identificationModule.orgStudyIdInfo.id",
    "org_study_type": "protocolSection.identificationModule.orgStudyIdInfo.type",
    "org_study_link": "protocolSection.identificationModule.orgStudyIdInfo.link",
    # Description
    "brief_summary": "protocolSection.descriptionModule.briefSummary",
    "detailed_description": "protocolSection.descriptionModule.detailedDescription",
    # Design (single values)
    "study_type": "protocolSection.designModule.studyType",
    "enrollment_type": "protocolSection.designModule.enrollmentInfo.type",
    "enrollment_count": "protocolSection.designModule.enrollmentInfo.count",
    "design_allocation": "protocolSection.designModule.designInfo.allocation",
    "design_intrv_model": "protocolSection.designModule.designInfo.interventionModel",
    "design_intrv_model_desc": "protocolSection.designModule.designInfo.interventionModelDescription",
    "design_primary_purpose": "protocolSection.designModule.designInfo.primaryPurpose",
    "design_obsv_model": "protocolSection.designModule.designInfo.observationalModel",
    "design_time_pers": "protocolSection.designModule.designInfo.timePerspective",
    "design_masking": "protocolSection.designModule.designInfo.maskingInfo.masking",
    "design_masking_desc": "protocolSection.designModule.designInfo.maskingInfo.maskingDescription",
    "design_who_masked": "protocolSection.designModule.designInfo.maskingInfo.whoMasked",
    # expanded access
    "exp_acc_type_intermediate": "protocolSection.designModule.expandedAccessTypes.intermediate",
    "exp_acc_type_treatment": "protocolSection.designModule.expandedAccessTypes.treatment",
    # Biospec
    "biospec_retention": "protocolSection.designModule.bioSpec.retention",
    "biospec_description": "protocolSection.designModule.bioSpec.description",
    # eligibility
    "eligibility_criteria": "protocolSection.eligibilityModule.eligibilityCriteria",
    "healthy_volunteers": "protocolSection.eligibilityModule.healthyVolunteers",
    "sex": "protocolSection.eligibilityModule.sex",
    "gender_based": "protocolSection.eligibilityModule.genderBased",
    "gender_description": "protocolSection.eligibilityModule.genderDescription",
    "min_age": "protocolSection.eligibilityModule.genderBased",
    "max_age": "protocolSection.eligibilityModule.sex",
    "population_desc": "protocolSection.eligibilityModule.studyPopulation",
    "sampling_method": "protocolSection.eligibilityModule.samplingMethod",
    # status
    "overall_status": "protocolSection.statusModule.overallStatus",
    "last_known_status": "protocolSection.statusModule.lastKnownStatus",
    "status_verified_date": "protocolSection.statusModule.statusVerifiedDate",
    "delayed_posting": "protocolSection.statusModule.delayedPosting",
    "start_date": "protocolSection.statusModule.startDateStruct.date",
    "start_date_type": "protocolSection.statusModule.startDateStruct.type",
    "first_submit_date": "protocolSection.statusModule.studyFirstSubmitDate",
    "first_submit_qc_date": "protocolSection.statusModule.studyFirstSubmitQcDate",
    "last_update_submit": "protocolSection.statusModule.lastUpdateSubmitDate",
    "completion_date": "protocolSection.statusModule.completionDateStruct.date",
    "completion_date_type": "protocolSection.statusModule.completionDateStruct.type",
    "why_stopped": "protocolSection.statusModule.whyStopped",
    "has_exp_access": "protocolSection.statusModule.expandedAccessInfo.hasExpandedAccess",
    # Oversight
    "has_dmc": "protocolSection.oversightModule.oversightHasDmc",
    "is_fda_regulated_drug": "protocolSection.oversightModule.isFdaRegulatedDrug",
    "is_fda_regulated_device": "protocolSection.oversightModule.isFdaRegulatedDevice",
    "is_unapproved_device": "protocolSection.oversightModule.isUnapprovedDevice",
    "is_us_export": "protocolSection.oversightModule.isUsExport",
    # individual participant data
    "ipd_sharing": "protocolSection.ipdSharingStatementModule.ipdSharing",
    "ipd_desc": "protocolSection.ipdSharingStatementModule.description",
    "ipd_time_frame": "protocolSection.ipdSharingStatementModule.timeFrame",
    "ipd_access_criteria": "protocolSection.ipdSharingStatementModule.accessCriteria",
    "ipd_url": "protocolSection.ipdSharingStatementModule.url",
    # # participant_flow
    # 'flow_pre_assignment_details': 'resultsSection.participantFlowModule.preAssignmentDetails',
    # 'flow_recruitment_details': 'resultsSection.participantFlowModule.recruitmentDetails',
    # 'flow_type_unit_analysed': 'resultsSection.participantFlowModule.typeUnitsAnalyzed',
    #
    # # baseline characteristics
    # 'bsln_pop_desc': 'resultsSection.baselineCharacteristicsModule.populationDescription',
    # 'bsln_pop_units_analysed': 'resultsSection.baselineCharacteristicsModule.typeUnitsAnalyzed',
    #
    # # adverse events
    # 'ae_freq_threshold': 'resultsSection.adverseEventsModule.frequencyThreshold',
    # 'ae_timeframe': 'resultsSection.adverseEventsModule.timeFrame',
    # 'ae_description': 'resultsSection.adverseEventsModule.description',
    # documents
    "large_doc_no_sap": "documentSection.largeDocumentModule.noSap",
    # Miscellaneous
    "version_holder": "derivedSection.miscInfoModule.versionHolder",
    "has_results": "hasResults",
    "last_updated": "protocolSection.statusModule.lastUpdatePostDateStruct.date",
    "unposted_responsible_party": "annotationSection.annotationModule.unpostedAnnotation.unpostedResponsibleParty",
    "limitations_description": "resultsSection.moreInfoModule.limitationsAndCaveats.description",
    "certain_agr_pi_sponsor_employee": "resultsSection.moreInfoModule.certainAgreement.piSponsorEmployee",
    "certain_agr_restriction_type": "resultsSection.moreInfoModule.certainAgreement.restrictionType",
    "certain_agr_restrictive": "resultsSection.moreInfoModule.certainAgreement.restrictiveAgreement",
    "certain_agr_other_details": "resultsSection.moreInfoModule.certainAgreement.otherDetails",
    "poc_title": "resultsSection.moreInfoModule.pointOfContact.title",
    "poc_organization": "resultsSection.moreInfoModule.pointOfContact.organization",
    "poc_email": "resultsSection.moreInfoModule.pointOfContact.email",
    "poc_phone": "resultsSection.moreInfoModule.pointOfContact.phone",
    "poc_phone_ext": "resultsSection.moreInfoModule.pointOfContact.phoneExt",
    "submission_tracking_est_results_date": "derivedSection.miscInfoModule.submissionTracking.estimatedResultsFirstSubmitDate",
    "submission_tracking_first_mcp_date": "derivedSection.miscInfoModule.submissionTracking.firstMcpInfo.postDateStruct.date",
    "submission_tracking_first_mcp_type": "derivedSection.miscInfoModule.submissionTracking.firstMcpInfo.postDateStruct.type",
}


ONE_TO_ONE_TABLES = {  # one to on fields requiring a separate dim
    "lead_sponsors": {
        "path": "protocolSection.sponsorCollaboratorsModule.leadSponsor",
        "col_paths": [
            "protocolSection.sponsorCollaboratorsModule.leadSponsor.name",
            "protocolSection.sponsorCollaboratorsModule.leadSponsor.class",
        ],
        "type": "simple dict",
        "dim_table": "lead_sponsors",
    }
}


ONE_TO_MANY_FIELDS = {
    "collaborators": {
        "path": "protocolSection.sponsorCollaboratorsModule.collaborator",
        "type": "array_of_dicts",
        "dim_table": "collaborator_sponsor",
        "bridge_table": "study_collaborators",
        "extract_fields": ["name", "class"],
    },
    "conditions": {
        "path": "protocolSection.conditionsModule.conditions",
        "entity": "condition",
        "type": "simple_array",
        "dim_table": "conditions",
        "bridge_table": "study_conditions",
        "key_fields": ["condition_name"],
    },
    "keywords": {
        "path": "protocolSection.conditionsModule.keywords",
        "entity": "keyword",
        "type": "simple_array",
        "dim_table": "keywords",
        "bridge_table": "study_keywords",
        "key_fields": ["condition_keyword"],
    },
    "interventions": {
        "path": "protocolSection.armsInterventionsModule.interventions",
        "entity": "intervention",
        "type": "array_of_dicts",
        "extract_fields": ["type", "name", "description"],
        "explode_fields": ["otherNames"],
        "dim_table": "interventions",
        "bridge_table": "study_interventions",
    },
    "intervention_other_names": {
        "parent_path": "protocolSection.armsInterventionsModule.interventions",
        "nested_path": "otherNames",
        "type": "nested_simple_array",
        "bridge_table": "intervention_other_names",
    },
    "arm_groups": {
        "path": "protocolSection.armsInterventionsModule.armGroups",
        "entity": "arm_group",
        "type": "array_of_dicts",
        "dim_table": "study_arm_groups",
        "extract_fields": ["label", "type", "description"],
        "explode_fields": ["interventionNames"],
    },
    "arm_group_interventions": {
        "parent_path": "protocolSection.armsInterventionsModule.armGroups",
        "nested_path": "interventionNames",
        "type": "nested_simple_array",
        "bridge_table": "arm_group_interventions",
    },
    "locations": {
        "path": "protocolSection.contactsLocationsModule.locations",
        "entity": "site",
        "type": "array_of_dicts",
        "dim_table": "sites",
        "bridge_table": "study_sites",
        "extract_fields": ["facility", "city", "state", "zip", "country", "status"],
        "nested_objects": {"geoPoint": ["lat", "lon"]},
    },
    "location_contacts": {
        "parent_path": "protocolSection.contactsLocationsModule.locations",
        "nested_path": "contacts",
        "type": "nested_array_of_dicts",
        "dim_table": "location_contacts",
        "extract_fields": ["name", "role", "email", "phone", "phoneExt"],
    },
    "central_contacts": {
        "path": "protocolSection.contactsLocationsModule.centralContacts",
        "type": "array_of_dicts",
        "dim_table": "contacts",
        "bridge_table": "study_contacts",
        "extract_fields": ["name", "role", "email", "phone", "phoneExt"],
    },
    "overall_officials": {
        "path": "protocolSection.contactsLocationsModule.overallOfficials",
        "entity": "investigator",
        "type": "array_of_dicts",
        "dim_table": "investigators",
        "bridge_table": "study_investigators",
        "extract_fields": ["name", "affiliation", "role"],
    },
    "primary_outcomes": {
        "path": "protocolSection.outcomesModule.primaryOutcomes",
        "type": "array_of_dicts",
        "dim_table": "study_outcomes",
        "extract_fields": ["measure", "description", "timeFrame"],
        "outcome_type": "PRIMARY",
    },
    "secondary_outcomes": {
        "path": "protocolSection.outcomesModule.secondaryOutcomes",
        "type": "array_of_dicts",
        "dim_table": "study_outcomes",
        "extract_fields": ["measure", "description", "timeFrame"],
        "outcome_type": "SECONDARY",
    },
    "other_outcomes": {
        "path": "protocolSection.outcomesModule.otherOutcomes",
        "type": "array_of_dicts",
        "dim_table": "study_outcomes",
        "extract_fields": ["measure", "description", "timeFrame"],
        "outcome_type": "OTHER",
    },
    "references": {
        "path": "protocolSection.referencesModule.references",
        "type": "array_of_dicts",
        "dim_table": "study_publications",
        "extract_fields": ["pmid", "type", "citation"],
    },
    "retractions": {
        "path": "protocolSection.referencesModule.retractions",
        "type": "array_of_dicts",
        "dim_table": "study_retractions",
        "extract_fields": ["pmid", "status"],
    },
    "see_also": {
        "path": "protocolSection.referencesModule.seeAlsoLinks",
        "type": "array_of_dicts",
        "dim_table": "study_see_also",
        "extract_fields": ["label", "url"],
    },
    "phases": {
        "path": "protocolSection.designModule.phases",
        "type": "simple_array",
        "dim_table": "phases",
        "bridge_table": "study_phases",
        "key_fields": ["phase"],
    },
    "who_masked": {
        "path": "protocolSection.designModule.designInfo.maskingInfo.whoMasked",
        "type": "simple_array",
        "dim_table": "masked_roles",
        "bridge_table": "study_masked_roles",
        "key_fields": ["role"],
    },
    "std_ages": {
        "path": "protocolSection.eligibilityModule.stdAges",
        "type": "simple_array",
        "dim_table": "age_groups",
        "bridge_table": "study_age_groups",
        "key_fields": ["age_group"],
    },
    "ipd_info_types": {
        "path": "protocolSection.ipdSharingStatementModule.infoTypes",
        "type": "simple_array",
        "dim_table": "ipd_info_types",
        "bridge_table": "study_ipd_info_types",
        "key_fields": ["info_type"],
    },
    "secondary_id_infos": {
        "path": "protocolSection.identificationModule.secondaryIdInfos",
        "type": "array_of_dicts",
        "dim_table": "secondary_ids",
        "bridge_table": "study_secondary_ids",
        "extract_fields": ["id", "type", "domain", "link"],
    },
    "nct_id_aliases": {
        "path": "protocolSection.identificationModule.nctIdAliases",
        "type": "simple_array",
        "dim_table": "nct_aliases",
        "bridge_table": "study_nct_aliases",
        "key_fields": ["alias_nct_id"],
    },
    # ===== DERIVED SECTION (MeSH) =====
    # CONDITION MESH TERMS
    "condition_mesh_terms": {
        "path": "derivedSection.conditionBrowseModule.meshes",
        "type": "array_of_dicts",
        "dim_table": "mesh_terms",
        "bridge_table": "study_mesh_conditions",
        "extract_fields": ["id", "term"],
        "is_primary": True,
    },
    "condition_mesh_ancestors": {
        "path": "derivedSection.conditionBrowseModule.ancestors",
        "type": "array_of_dicts",
        "dim_table": "mesh_terms",
        "bridge_table": "study_mesh_conditions",
        "extract_fields": ["id", "term"],
        "is_primary": False,
    },
    "intervention_mesh_terms": {
        "path": "derivedSection.interventionBrowseModule.meshes",
        "type": "array_of_dicts",
        "dim_table": "mesh_terms",
        "bridge_table": "study_mesh_interventions",
        "extract_fields": ["id", "term"],
        "mesh_category": "intervention",
        "is_primary": True,
    },
    "intervention_mesh_ancestors": {
        "path": "derivedSection.interventionBrowseModule.ancestors",
        "type": "array_of_dicts",
        "dim_table": "mesh_terms",
        "bridge_table": "study_mesh_interventions",
        "extract_fields": ["id", "term"],
        "mesh_category": "intervention",
        "is_primary": False,
    },
    "large_documents": {
        "path": "documentSection.largeDocumentModule.largeDocs",
        "type": "array_of_dicts",
        "dim_table": "study_documents",
        "extract_fields": [
            "typeAbbrev",
            "hasProtocol",
            "hasSap",
            "hasIcf",
            "label",
            "date",
            "uploadDate",
            "filename",
            "size",
        ],
    },
    "unposted_events": {
        "path": "annotationSection.annotationModule.unpostedAnnotation.unpostedEvents",
        "type": "array_of_dicts",
        "dim_table": "unposted_events",
        "bridge_table": "study_unposted_events",
        "extract_fields": ["type", "date", "dateUnknown"],
    },
    "violation_events": {
        "path": "annotationSection.annotationModule.violationAnnotation.violationEvents",
        "type": "array_of_dicts",
        "dim_table": "violation_events",
        "bridge_table": "study_violation_events",
        "extract_fields": [
            "type",
            "description",
            "creationDate",
            "issuedDate",
            "releaseDate",
            "postedDate",
        ],
    },
    "removed_countries": {
        "path": "derivedSection.miscInfoModule.removedCountries",
        "type": "simple_array",
        "dim_table": "removed_countries",
        "bridge_table": "study_removed_countries",
        "key_fields": ["country"],
    },
    "submission_infos": {
        "path": "derivedSection.miscInfoModule.submissionTracking.submissionInfos",
        "type": "array_of_dicts",
        "dim_table": "submission_tracking",
        "bridge_table": "study_submission_tracking",
        "extract_fields": [
            "releaseDate",
            "unreleaseDate",
            "unreleaseDateUnknown",
            "resetDate",
            "mcpReleaseN",
        ],
    },
    # #RESULTS SECTION
    #
    # # PARTICIPANT FLOW GROUPS
    # 'participant_flow_groups': {
    #     'path': 'resultsSection.participantFlowModule.groups',
    #     'type': 'array_of_dicts',
    #     'dim_table': 'flow_groups',
    #     'bridge_table': 'study_flow_groups',
    #     'extract_fields': ['id', 'title', 'description']
    # },
    #
    # # PARTICIPANT FLOW PERIODS
    # 'participant_flow_periods': {
    #     'path': 'resultsSection.participantFlowModule.periods',
    #     'type': 'array_of_dicts',
    #     'dim_table': 'flow_periods',
    #     'bridge_table': 'study_flow_periods',
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
    #     'dim_table': 'baseline_groups',
    #     'bridge_table': 'study_baseline_groups',
    #     'extract_fields': ['id', 'title', 'description']
    # },
    #
    # # BASELINE DENOMS
    # 'baseline_denoms': {
    #     'path': 'resultsSection.baselineCharacteristicsModule.denoms',
    #     'type': 'array_of_dicts',
    #     'dim_table': 'baseline_denoms',
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
    #     'dim_table': 'baseline_measures',
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
    #     'dim_table': 'outcome_measure_results',
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
    #     'dim_table': 'ae_groups',
    #     'bridge_table': 'study_ae_groups',
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
    #     'dim_table': 'adverse_events',
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
    #     'dim_table': 'adverse_events',
    #     'extract_fields': [
    #         'term', 'organSystem', 'sourceVocabulary',
    #         'assessmentType', 'notes'
    #     ],
    #     'nested_arrays': {
    #         'stats': ['groupId', 'numEvents', 'numAffected', 'numAtRisk']
    #     },
    #     'severity': 'OTHER'
    # },
}
