SINGLE_FIELDS = {
    # Identification
    "nct_id": "protocolSection.identificationModule.nctId",
    "brief_title": "protocolSection.identificationModule.briefTitle",
    "official_title": "protocolSection.identificationModule.officialTitle",
    "acronym": "protocolSection.identificationModule.acronym",
    "org_study_id": "protocolSection.identificationModule.orgStudyIdInfo.id",
    # Description
    "brief_summary": "protocolSection.descriptionModule.briefSummary",
    "detailed_desc": "protocolSection.descriptionModule.detailedDescription",

    # Sponsor
    "responsible_party": "protocolSection.sponsorCollaboratorsModule.responsibleParty.type",

    # Design (single values)
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
    "min_age": "protocolSection.eligibilityModule.minimumAge",
    "max_age": "protocolSection.eligibilityModule.maximumAge",
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
    # Miscellaneous
    "version_holder": "derivedSection.miscInfoModule.versionHolder",
    "has_results": "hasResults",
    "last_updated": "protocolSection.statusModule.lastUpdatePostDateStruct.date",
    "limitations_desc": "resultsSection.moreInfoModule.limitationsAndCaveats.description",
    # Certain agreements
    "certain_agreement_pi_sponsor_employee": "resultsSection.moreInfoModule.certainAgreement.piSponsorEmployee",
    "certain_agreement_restrictive": "resultsSection.moreInfoModule.certainAgreement.restrictiveAgreement",
    "certain_agreement_other_details": "resultsSection.moreInfoModule.certainAgreement.otherDetails",
    "certain_agreement_restriction_type": "resultsSection.moreInfoModule.certainAgreement.restrictionType",
    # Point of contact
    "poc_title": "resultsSection.moreInfoModule.pointOfContact.title",
    "poc_organization": "resultsSection.moreInfoModule.pointOfContact.organization",
    "poc_email": "resultsSection.moreInfoModule.pointOfContact.email",
    "poc_phone": "resultsSection.moreInfoModule.pointOfContact.phone",
    "poc_phone_ext": "resultsSection.moreInfoModule.pointOfContact.phoneExt",
    # Submission tracking
    "sub_tracking_estimated_results_date": "derivedSection.miscInfoModule.submissionTracking.estimatedResultsFirstSubmitDate",
}

NESTED_FIELDS = {
    "sponsor": { #NOT NESTED BUT TREATED AS A SEPARATE DIM
        "index_field": "protocolSection.sponsorCollaboratorsModule.leadSponsor",
        "object_type": "simple dict",
        "fields": [
            ("lead_sponsor_name", "name"),
            ("lead_sponsor_class", "class"),
        ],
        "table_name": "sponsors",
        "bridge_table_name": "study_sponsors",
        "transformer_method": "extract_sponsors",
    },
    "collaborators": {
        "index_field": "protocolSection.sponsorCollaboratorsModule.collaborators",
        "object_type": "array_of_dicts",
        "fields": [
            ("sponsor_name", "name"),
            ("sponsor_class", "class"),
        ],
        "table_name": "sponsor",
        "bridge_table_name": "study_sponsors",
        "transformer_method": "extract_sponsors",
    },
    "conditions": {
        "index_field": "protocolSection.conditionsModule.conditions",
        "object_type": "simple_array",
        "table_name": "conditions",
        "bridge_table_name": "bridge_study_conditions",
        "field_name": "condition_name",
        "transformer_method": "extract_conditions",
    },
    "keywords": {
        "index_field": "protocolSection.conditionsModule.keywords",
        "object_type": "simple_array",
        "table_name": "keywords",
        "bridge_table_name": "bridge_study_keywords",
        "field_name": "keyword",
        "transformer_method": "extract_keywords",
    },
    "interventions": {
        "index_field": "protocolSection.armsInterventionsModule.interventions",
        "object_type": "array_of_dicts",
        "fields": [
            ("intervention_name", "name"),
            ("intervention_desc", "description"),
            ("intervention_type", "type"),
        ],
        "table_name": "interventions",
        "bridge_table_name": "bridge_study_interventions",
        "transformer_method": "extract_interventions",
    },
    "arm_groups": {
        "index_field": "protocolSection.armsInterventionsModule.armGroups",
        "object_type": "array_of_dicts",
        "table_name": "study_arm_group_interventions",
        "fields": [
            ("arm_group_label", "label"),
            ("arm_group_type", "type"),
            ("arm_group_desc", "description"),
        ],
        "transformer_method": "extract_arm_groups",
    },

    "central_contacts": {
        "index_field": "protocolSection.contactsLocationsModule.centralContacts",
        "object_type": "array_of_dicts",
        "table_name": "contacts",
        "bridge_table_name": "study_contacts",
        "fields": [
            ("name", "name"),
            ("role", "role"),
            ("email", "email"),
            ("phone", "phone"),
            ("phoneExt", "phoneExt"),
        ],
        "transformer_method": "extract_central_contacts",
    },
    "locations": {
        "index_field": "protocolSection.contactsLocationsModule.locations",
        "object_type": "array_of_dicts",
        "table_name": "sites",
        "bridge_table_name": "study_sites",
        "fields": [
            ("site_facility", "facility"),
            ("city", "city"),
            ("state", "state"),
            ("zip", "zip"),
            ("country", "country"),
            ("site_status", "status"),
        ],
        "nested": {
            "geoPoint": {
                "object_type": "simple_dict",
                "fields": ["lat", "lon"],
            },
            #contacts are saved as a JSON blob
            "contacts": {
                "object_type": "nested_array_of_dicts",
                "table_name": "contacts",
                "bridge_table_name": "location_contacts",
                "fields": [
                    ("name", "name"),
                    ("role", "role"),
                    ("email", "email"),
                    ("phone", "phone"),
                    ("phoneExt", "phoneExt"),
                ],
                "transformer_method": "extract_contacts",
            },
        },
        "transformer_method": "extract_locations",
    },
    #REFERENCES MODULE
    "references": {
        "index_field": "protocolSection.referencesModule.references",
        "object_type": "array_of_dicts",
        "table_name": "study_publications",
        "extract_fields": ["pmid", "object_type", "citation"],
    },
    "retractions": {
        "index_field": "protocolSection.referencesModule.retractions",
        "object_type": "array_of_dicts",
        "table_name": "study_retractions",
        "fields": [("pmid", "pmid"), ("status", "status")],
        "transformer_method": "extract_outcomes",
    },
    "see_also": {
        "index_field": "protocolSection.referencesModule.seeAlsoLinks",
        "object_type": "array_of_dicts",
        "table_name": "study_see_also",
        "fields": [("label", "label"), ("url", "url")],
        "transformer_method": "extract_see_also",
    },








    "primary_outcomes": {
        "index_field": "protocolSection.outcomesModule.primaryOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame"),
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "PRIMARY",
    },
    "secondary_outcomes": {
        "index_field": "protocolSection.outcomesModule.secondaryOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame"),
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "SECONDARY",
    },
    "other_outcomes": {
        "index_field": "protocolSection.outcomesModule.otherOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame"),
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "OTHER",
    },

    "phases": {
        "index_field": "protocolSection.designModule.phases",
        "object_type": "simple_array",
        "table_name": "phases",
        "bridge_table_name": "study_phases",
        "field_name": "phase",
        "transformer_method": "extract_study_phases",
    },

    "ipd_info_types": {
        "index_field": "protocolSection.ipdSharingStatementModule.infoTypes",
        "object_type": "simple_array",
        "table_name": "ipd_info_types",
        "bridge_table_name": "study_ipd_info_types",
        "field_name": "info_type",
        "transformer_method": "extract_ipd_info_types",
    },
    "secondary_id_infos": {
        "index_field": "protocolSection.identificationModule.secondaryIdInfos",
        "object_type": "array_of_dicts",
        "table_name": "secondary_ids",
        "bridge_table_name": "study_secondary_ids",
        "fields": [
            ("id", "id"),
            ("object_type", "object_type"),
            ("domain", "domain"),
            ("link", "link"),
        ],
        "transformer_method": "extract_id_infos",
    },
    "nct_id_aliases": {
        "index_field": "protocolSection.identificationModule.nctIdAliases",
        "object_type": "simple_array",
        "table_name": "nct_aliases",
        "bridge_table_name": "study_nct_aliases",
        "field_name": "alias_nct_id",
        "transformer_method": "extract_nct_id_aliases",
    },
    # ===== DERIVED SECTION (MeSH) =====
    # CONDITION MESH TERMS
    "condition_mesh_terms": {
        "index_field": "derivedSection.conditionBrowseModule.meshes",
        "object_type": "array_of_dicts",
        "fields": [("id", "id"), ("term", "term")],
        "table_name": "condition_mesh_terms",
        "bridge_table_name": "study_conditions_mesh",
        "is_primary": True,
        "transformer_method": "extract_condition_mesh",
    },
    "condition_mesh_ancestors": {
        "index_field": "derivedSection.conditionBrowseModule.ancestors",
        "object_type": "array_of_dicts",
        "fields": [("id", "id"), ("term", "term")],
        "table_name": "condition_mesh_terms",
        "bridge_table_name": "study_conditions_mesh",
        "is_primary": False,
        "transformer_method": "extract_condition_mesh",
    },
    "intervention_mesh_terms": {
        "index_field": "derivedSection.interventionBrowseModule.meshes",
        "object_type": "array_of_dicts",
        "fields": [("id", "id"), ("term", "term")],
        "table_name": "intervention_mesh_terms",
        "bridge_table_name": "study_interventions_mesh",
        "is_primary": True,
        "transformer_method": "extract_intervention_mesh",
    },
    "intervention_mesh_ancestors": {
        "index_field": "derivedSection.interventionBrowseModule.ancestors",
        "object_type": "array_of_dicts",
        "fields": [("id", "id"), ("term", "term")],
        "table_name": "intervention_mesh_terms",
        "bridge_table_name": "study_interventions_mesh",
        "is_primary": False,
        "transformer_method": "extract_intervention_mesh",
    },
    "large_documents": {
        "index_field": "documentSection.largeDocumentModule.largeDocs",
        "object_type": "array_of_dicts",
        "fields": [
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
        "table_name": "study_documents",
        "transformer_method": "extract_large_documents",
    },
    "unposted_events": {
        "index_field": "annotationSection.annotationModule.unpostedAnnotation.unpostedEvents",
        "object_type": "array_of_dicts",
        "fields": [
            ("object_type", "object_type"),
            ("date", "date"),
            ("dateUnknown", "dateUnknown"),
        ],
        "table_name": "unposted_events",
        "bridge_table_name": "study_unposted_events",
        "transformer_method": "extract_unposted_events",
    },
    "violation_events": {
        "index_field": "annotationSection.annotationModule.violationAnnotation.violationEvents",
        "object_type": "array_of_dicts",
        "fields": [
            ("object_type", "object_type"),
            ("description", "description"),
            ("creationDate", "creationDate"),
            ("issuedDate", "issuedDate"),
            ("releaseDate", "releaseDate"),
            ("postedDate", "postedDate"),
        ],
        "table_name": "violation_events",
        "bridge_table_name": "study_violation_events",
        "transformer_method": "extract_violation_events",
    },
    "removed_countries": {
        "index_field": "derivedSection.miscInfoModule.removedCountries",
        "object_type": "simple_array",
        "table_name": "countries",
        "bridge_table_name": "study_removed_countries",
        "field_name": "country",
        "transformer_method": "extract_removed_countries",
    },
    "sub_infos": {
        "index_field": "derivedSection.miscInfoModule.submissionTracking.submissionInfos",
        "object_type": "array_of_dicts",
        "fields": [
            ("releaseDate", "releaseDate"),
            ("unreleaseDate", "unreleaseDate"),
            ("unreleaseDateUnknown", "unreleaseDateUnknown"),
            ("resetDate", "resetDate"),
            ("mcpReleaseN", "mcpReleaseN"),
        ],
        "table_name": "sub_tracking",
        "bridge_table_name": "study_sub_tracking",
        "transformer_method": "extract_sub_infos",
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
