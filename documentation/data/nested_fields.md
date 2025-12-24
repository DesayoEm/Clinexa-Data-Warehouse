NESTED_FIELDS = {

  "sponsors": {
        "path": "protocolSection.sponsorCollaboratorsModule.leadSponsor",
        "object_type": "simple dict",
        "fields": [
            ("lead_sponsor_name", "name"),
            ("lead_sponsor_class", "class"),
        ],
        "table_name": "sponsors",
        "bridge_table_name": "study_sponsors",
        "transformer_method": "extract_sponsors"
    },

    "collaborators": {
        "path": "protocolSection.sponsorCollaboratorsModule.collaborator",
        "object_type": "array_of_dicts",
        "fields": [
            ("sponsor_name", "name"),
            ("sponsor_class", "class"),
        ],
        "table_name": "sponsor",
        "bridge_table_name": "study_sponsors",
        "transformer_method": "extract_sponsors"
    },
    
    "conditions": {
        "path": "protocolSection.conditionsModule.conditions",
        "object_type": "simple_array",
        "table_name": "conditions",
        "bridge_table_name": "study_conditions",
        "field_name": "condition_name",
        "transformer_method":"extract_conditions"
    },
    
    "keywords": {
        "path": "protocolSection.conditionsModule.keywords",
        "entity": "keyword",
        "object_type": "simple_array",
        "table_name": "keywords",
        "bridge_table_name": "study_keywords",
        "field_name": "study_keyword",
        "transformer_method":"extract_keywords"
    },

    "interventions": {
        "path": "protocolSection.armsInterventionsModule.interventions",
        "entity": "intervention",
        "object_type": "array_of_dicts",
        "fields": [
            ("intervention_name", "name"),
            ("intervention_description", "description"),
            ("object_type", "object_type"),
        ],
        "nested": {
            "nested_path": {
                "object_type": "nested_simple_array",
                "table_name": "intervention_other_names",
                "field_name": "intervention_other_name"
        },
        "table_name": "interventions",
        "bridge_table_name": "study_interventions",
        "transformer_method": "extract_interventions"
        }
    },

    "arm_groups": {
        "path": "protocolSection.armsInterventionsModule.armGroups",
        "object_type": "array_of_dicts",
        "table_name": "study_arm_groups",
        "fields": [
            ("arm_group_label", "label"),
            ("arm_group_description", "description"),
            ("object_type", "object_type"),
        ],
        "nested": {
            "interventionNames": {
                "object_type": "nested_simple_array",
                "bridge_table_name": "arm_group_interventions",
                "field_name": "intervention_name"
        },
        "transformer_method": "extract_arm_groups"
        }
    },

    "locations": {
        "path": "protocolSection.contactsLocationsModule.locations",
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
                "object_type": "simple_array",
                 "fields": ["lat", "lon"],
            },

            "contacts": {
                "object_type": "nested_array_of_dicts",
                "table_name": "contacts",
                "bridge_table_name": "location_contacts",
                "fields": [
                    ("name", "name"),
                    ("role", "role"),
                    ("email", "email"),
                    ("phone", "phone"),
                    ("phoneExt", "phoneExt")
                ]
            }
        },
        "transformer_method": "extract_contacts"
    },

    "central_contacts": {
        "path": "protocolSection.contactsLocationsModule.centralContacts",
        "object_type": "array_of_dicts",
        "table_name": "contacts",
        "bridge_table_name": "study_contacts",
        "fields": [
            ("name", "name"),
            ("role", "role"),
            ("email", "email"),
            ("phone", "phone"),
            ("phoneExt", "phoneExt")
        ],
        "transformer_method": "extract_contacts"
    },

    "overall_officials": {
        "path": "protocolSection.contactsLocationsModule.overallOfficials",
        "object_type": "array_of_dicts",
        "table_name": "investigators",
        "bridge_table_name": "study_investigators",
        "fields": [
            ("name", "name"),
            ("affiliation", "affiliation"),
            ("role", "role")
        ],
        "transformer_method": "extract_officials"
    },

    "primary_outcomes": {
        "path": "protocolSection.outcomesModule.primaryOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "PRIMARY",
    },

    "secondary_outcomes": {
        "path": "protocolSection.outcomesModule.secondaryOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "SECONDARY",
    },

    "other_outcomes": {
        "path": "protocolSection.outcomesModule.otherOutcomes",
        "object_type": "array_of_dicts",
        "table_name": "study_outcomes",
        "fields": [
            ("measure", "measure"),
            ("description", "description"),
            ("timeFrame", "timeFrame")
        ],
        "transformer_method": "extract_outcomes",
        "outcome_type": "OTHER",
    },

    "references": {
        "path": "protocolSection.referencesModule.references",
        "object_type": "array_of_dicts",
        "table_name": "study_publications",
        "extract_fields": ["pmid", "object_type", "citation"],
    },

    "retractions": {
        "path": "protocolSection.referencesModule.retractions",
        "object_type": "array_of_dicts",
        "table_name": "study_retractions",
        "fields": [
            ("pmid", "pmid"),
            ("status", "status")
        ],
        "transformer_method": "extract_outcomes",
    },

    "see_also": {
        "path": "protocolSection.referencesModule.seeAlsoLinks",
        "object_type": "array_of_dicts",
        "table_name": "study_see_also",
        "fields": [
            ("label", "label"),
            ("url", "url")
        ],
        "transformer_method": "extract_see_also"
    },

    "phases": {
        "path": "protocolSection.designModule.phases",
        "object_type": "simple_array",
        "table_name": "phases",
        "bridge_table_name": "study_phases",
        "field_name": "phase",
        "transformer_method": "extract_study_phases"
    },


    "std_ages": {
        "path": "protocolSection.eligibilityModule.stdAges",
        "object_type": "simple_array",
        "table_name": "age_groups",
        "bridge_table_name": "study_age_groups",
        "field_name": "age_group",
        "transformer_method": "extract_age_group"
    },

    "ipd_info_types": {
        "path": "protocolSection.ipdSharingStatementModule.infoTypes",
        "object_type": "simple_array",
        "table_name": "ipd_info_types",
        "bridge_table_name": "study_ipd_info_types",
        "field_name": "info_type",
        "transformer_method": "extract_ipd_info_types"
    },
#NOT EVERYTHING NEEDS A BRIDGE
    "secondary_id_infos": {
        "path": "protocolSection.identificationModule.secondaryIdInfos",
        "object_type": "array_of_dicts",
        "table_name": "secondary_ids",
        "bridge_table_name": "study_secondary_ids",
        "fields": [
            ("id", "id"),
            ("object_type", "object_type"),
            ("domain", "domain"),
            ("link", "link")
        ],
        "transformer_method": "extract_id_infos"

    },
    "nct_id_aliases": {
        "path": "protocolSection.identificationModule.nctIdAliases",
        "object_type": "simple_array",
        "table_name": "nct_aliases",
        "bridge_table_name": "study_nct_aliases",
        "field_name": "alias_nct_id",
        "transformer_method": "extract_nct_id_aliases"
    },

    # ===== DERIVED SECTION (MeSH) =====
    # CONDITION MESH TERMS


    "condition_mesh_terms": {
        "path": "derivedSection.conditionBrowseModule.meshes",
        "object_type": "array_of_dicts",
        "fields": [
            ("id", "id"),
            ("term", "term")
        ],
        "table_name": "condition_mesh_terms",
        "bridge_table_name": "study_conditions_mesh",
        "is_primary": True,
        "transformer_method": "extract_condition_mesh"
    },

    "condition_mesh_ancestors": {
        "path": "derivedSection.conditionBrowseModule.ancestors",
        "object_type": "array_of_dicts",
        "fields": [
            ("id", "id"),
            ("term", "term")
        ],
        "table_name": "condition_mesh_terms",
        "bridge_table_name": "study_conditions_mesh",
        "is_primary": False,
        "transformer_method": "extract_condition_mesh"
    },

    "intervention_mesh_terms": {
        "path": "derivedSection.interventionBrowseModule.meshes",
        "object_type": "array_of_dicts",
        "fields": [
            ("id", "id"),
            ("term", "term")
        ],
        "table_name": "intervention_mesh_terms",
        "bridge_table_name": "study_interventions_mesh",
        "is_primary": True,
        "transformer_method": "extract_intervention_mesh"
    },

    "intervention_mesh_ancestors": {
        "path": "derivedSection.interventionBrowseModule.ancestors",
        "object_type": "array_of_dicts",
        "fields": [
            ("id", "id"),
            ("term", "term")
        ],
        "table_name": "intervention_mesh_terms",
        "bridge_table_name": "study_interventions_mesh",
        "is_primary": False,
        "transformer_method": "extract_intervention_mesh"
    },

    "large_documents": {
        "path": "documentSection.largeDocumentModule.largeDocs",
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
        "transformer_method": "extract_large_documents"
    },

    "unposted_events": {
        "path": "annotationSection.annotationModule.unpostedAnnotation.unpostedEvents",
        "object_type": "array_of_dicts",
        "fields": [
            ("object_type", "object_type"),
            ("date", "date"),
            ("dateUnknown", "dateUnknown"),
        ],
        "table_name": "unposted_events",
        "bridge_table_name": "study_unposted_events",
        "transformer_method": "extract_unposted_events"
    },

    "violation_events": {
        "path": "annotationSection.annotationModule.violationAnnotation.violationEvents",
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
        "transformer_method": "extract_violation_events"
    },

    "removed_countries": {
        "path": "derivedSection.miscInfoModule.removedCountries",
        "object_type": "simple_array",
        "table_name": "countries",
        "bridge_table_name": "study_removed_countries",
        "field_name": "country",
        "transformer_method": "extract_removed_countries"
    },

    "submission_infos": {
        "path": "derivedSection.miscInfoModule.submissionTracking.submissionInfos",
        "object_type": "array_of_dicts",
        "fields": [
            ("releaseDate", "releaseDate"),
            ("unreleaseDate", "unreleaseDate"),
            ("unreleaseDateUnknown", "unreleaseDateUnknown"),
            ("resetDate", "resetDate"),
            ("mcpReleaseN", "mcpReleaseN")
        ],
        "table_name": "submission_tracking",
        "bridge_table_name": "study_submission_tracking",
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
