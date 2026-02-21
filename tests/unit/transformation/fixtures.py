import pytest
import pandas as pd


@pytest.fixture
def study_key():
    """Standard study key for testing."""
    return "abc123def456"


@pytest.fixture
def nct_id():
    """NCT ID for testing."""
    return "NCT12345678"


@pytest.fixture
def empty_study_data():
    """Empty study data series."""
    return pd.Series({})


@pytest.fixture
def minimal_study_data():
    """Minimal study data with just required fields."""
    return pd.Series(
        {
            "protocolSection.identificationModule.nctId": "NCT12345678",
            "protocolSection.identificationModule.briefTitle": "Test Study",
        }
    )


@pytest.fixture
def full_study_data():
    """Comprehensive study data with all modules populated."""
    return pd.Series(
        {
            # Identification Module
            "protocolSection.identificationModule.nctId": "NCT12345678",
            "protocolSection.identificationModule.briefTitle": "Test Clinical Trial",
            "protocolSection.identificationModule.officialTitle": "A Phase 3 Randomized Controlled Trial",
            "protocolSection.identificationModule.acronym": "TCT",
            "protocolSection.identificationModule.orgStudyIdInfo.id": "ORG-001",
            "protocolSection.identificationModule.secondaryIdInfos": [
                {
                    "id": "SEC-001",
                    "type": "OTHER",
                    "domain": "NIH",
                    "link": "http://study.com",
                },
                {
                    "id": "SEC-002",
                    "type": "REGISTRY",
                    "domain": "EudraCT",
                    "link": None,
                },
            ],
            "protocolSection.identificationModule.nctIdAliases": [
                "NCT00000001",
                "NCT00000002",
            ],
            # Description Module
            "protocolSection.descriptionModule.briefSummary": "This is a test study.",
            "protocolSection.descriptionModule.detailedDescription": "Detailed description.",
            # Sponsor Collaborators Module
            "protocolSection.sponsorCollaboratorsModule.leadSponsor.name": "Test Pharma Inc",
            "protocolSection.sponsorCollaboratorsModule.leadSponsor.class": "INDUSTRY",
            "protocolSection.sponsorCollaboratorsModule.responsibleParty.type": "SPONSOR",
            "protocolSection.sponsorCollaboratorsModule.collaborators": [
                {"name": "University Hospital", "class": "OTHER"},
                {"name": "Research Institute", "class": "NIH"},
            ],
            # Conditions Module
            "protocolSection.conditionsModule.conditions": [
                "Type 2 Diabetes",
                "Hypertension",
            ],
            "protocolSection.conditionsModule.keywords": [
                "diabetes",
                "blood pressure",
                "metabolic",
            ],
            # Arms Interventions Module
            "protocolSection.armsInterventionsModule.armGroups": [
                {
                    "label": "Treatment Arm",
                    "type": "EXPERIMENTAL",
                    "description": "Receives active treatment",
                    "interventionNames": ["Drug: Metformin", "Drug: Lisinopril"],
                },
                {
                    "label": "Placebo Arm",
                    "type": "PLACEBO_COMPARATOR",
                    "description": "Receives placebo",
                    "interventionNames": ["Drug: Placebo"],
                },
            ],
            "protocolSection.armsInterventionsModule.interventions": [
                {
                    "name": "Metformin",
                    "type": "DRUG",
                    "description": "500mg twice daily",
                    "otherNames": ["Glucophage", "Fortamet"],
                },
                {
                    "name": "Lisinopril",
                    "type": "DRUG",
                    "description": "10mg once daily",
                    "otherNames": [],
                },
                {
                    "name": "Placebo",
                    "type": "DRUG",
                    "description": "Matching placebo",
                    "otherNames": None,
                },
            ],
            # Outcomes Module
            "protocolSection.outcomesModule.primaryOutcomes": [
                {
                    "measure": "HbA1c Change",
                    "description": "Change in HbA1c from baseline",
                    "timeFrame": "12 weeks",
                },
            ],
            "protocolSection.outcomesModule.secondaryOutcomes": [
                {
                    "measure": "Blood Pressure",
                    "description": "Change in systolic BP",
                    "timeFrame": "12 weeks",
                },
            ],
            "protocolSection.outcomesModule.otherOutcomes": [
                {
                    "measure": "Quality of Life",
                    "description": "SF-36 score",
                    "timeFrame": "12 weeks",
                },
            ],
            # Contacts Locations Module
            "protocolSection.contactsLocationsModule.centralContacts": [
                {
                    "name": "Dr. John Smith",
                    "role": "CONTACT",
                    "phone": "555-1234",
                    "email": "jsmith@example.com",
                },
            ],
            "protocolSection.contactsLocationsModule.locations": [
                {
                    "facility": "City Hospital",
                    "city": "Boston",
                    "state": "Massachusetts",
                    "country": "United States",
                    "status": "RECRUITING",
                    "geoPoint": {"lat": 42.3601, "lon": -71.0589},
                    "contacts": [
                        {
                            "name": "Dr. Olusola",
                            "role": "Coordinator",
                            "phone": "90909090",
                            "phone_ext": "+234",
                            "email": "olusola@hospital.com",
                        },
                        {
                            "name": "Dr. Vishal",
                            "role": "Coordinator",
                            "phone": "90908080",
                            "phone_ext": "+234",
                            "email": "vish@hospital.com",
                        },
                    ],
                },
            ],
            # References Module
            "protocolSection.referencesModule.references": [
                {
                    "pmid": "12345678",
                    "type": "RESULT",
                    "citation": "Smith J et al. 2023",
                },
            ],
            "protocolSection.referencesModule.seeAlsoLinks": [
                {"label": "Study Website", "url": "http://study.example.com"},
            ],
            "protocolSection.referencesModule.availIpds": [
                {
                    "id": "IPD001",
                    "type": "STUDY_PROTOCOL",
                    "url": "http://ipd.example.com",
                    "comment": "Full protocol",
                },
            ],
            # Status Module
            "protocolSection.statusModule.overallStatus": "RECRUITING",
            "protocolSection.statusModule.startDateStruct.date": "2023-01-01",
            "protocolSection.statusModule.startDateStruct.type": "ACTUAL",
            "protocolSection.statusModule.completionDateStruct.date": "2026-12-31",
            "protocolSection.statusModule.completionDateStruct.type": "ESTIMATED",
            # Design Module
            "protocolSection.designModule.studyType": "INTERVENTIONAL",
            "protocolSection.designModule.enrollmentInfo.count": 500,
            "protocolSection.designModule.enrollmentInfo.type": "ESTIMATED",
            # Eligibility Module
            "protocolSection.eligibilityModule.eligibilityCriteria": "Adults 18-65",
            "protocolSection.eligibilityModule.healthyVolunteers": False,
            "protocolSection.eligibilityModule.sex": "ALL",
            "protocolSection.eligibilityModule.minimumAge": "18 Years",
            "protocolSection.eligibilityModule.maximumAge": "65 Years",
            # Has Results
            "hasResults": True,
        }
    )


@pytest.fixture
def results_study_data():
    """Study data with results section populated."""
    return pd.Series(
        {
            # Outcome Measures Module
            "resultsSection.outcomeMeasuresModule.outcomeMeasures": [
                {
                    "type": "PRIMARY",
                    "title": "HbA1c Change from Baseline",
                    "description": "Change in HbA1c at 12 weeks",
                    "populationDescription": "ITT population",
                    "reportingStatus": "POSTED",
                    "paramType": "MEAN",
                    "dispersionType": "STANDARD_DEVIATION",
                    "unitOfMeasure": "percent",
                    "timeFrame": "12 weeks",
                    "groups": [
                        {
                            "id": "OG000",
                            "title": "Treatment",
                            "description": "Active arm",
                        },
                        {
                            "id": "OG001",
                            "title": "Placebo",
                            "description": "Control arm",
                        },
                    ],
                    "denoms": [
                        {
                            "units": "participants",
                            "counts": [
                                {"groupId": "OG000", "value": "250"},
                                {"groupId": "OG001", "value": "248"},
                            ],
                        },
                    ],
                    "classes": [
                        {
                            "categories": [
                                {
                                    "measurements": [
                                        {
                                            "groupId": "OG000",
                                            "value": "-1.2",
                                            "spread": "0.8",
                                        },
                                        {
                                            "groupId": "OG001",
                                            "value": "-0.3",
                                            "spread": "0.7",
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                    "analyses": [
                        {
                            "paramType": "DIFFERENCE",
                            "paramValue": "-0.9",
                            "statisticalMethod": "t-test",
                            "pValue": "0.001",
                            "groupIds": ["OG000", "OG001"],
                        },
                    ],
                },
            ],
            # Participant Flow Module
            "resultsSection.participantFlowModule.groups": [
                {
                    "id": "FG000",
                    "title": "Treatment Arm",
                    "description": "Active treatment",
                },
                {
                    "id": "FG001",
                    "title": "Placebo Arm",
                    "description": "Placebo control",
                },
            ],
            "resultsSection.participantFlowModule.periods": [
                {
                    "title": "Screening",
                    "milestones": [
                        {
                            "type": "STARTED",
                            "comment": None,
                            "achievements": [
                                {
                                    "groupId": "FG000",
                                    "numSubjects": "300",
                                    "comment": None,
                                },
                                {
                                    "groupId": "FG001",
                                    "numSubjects": "295",
                                    "comment": None,
                                },
                            ],
                        },
                    ],
                    "dropWithdraws": [
                        {
                            "type": "Withdrawal",
                            "comment": None,
                            "reasons": [
                                {
                                    "groupId": "FG000",
                                    "numSubjects": "5",
                                    "comment": "Adverse event",
                                },
                            ],
                        },
                    ],
                },
            ],
            # Adverse Events Module
            "resultsSection.adverseEventsModule.description": "Safety analysis population",
            "resultsSection.adverseEventsModule.frequencyThreshold": "5",
            "resultsSection.adverseEventsModule.timeFrame": "12 weeks",
            "resultsSection.adverseEventsModule.eventGroups": [
                {
                    "id": "EG000",
                    "title": "Treatment",
                    "description": "Active treatment arm",
                    "deathsNumAffected": 0,
                    "deathsNumAtRisk": 250,
                    "seriousNumAffected": 5,
                    "seriousNumAtRisk": 250,
                    "otherNumAffected": 45,
                    "otherNumAtRisk": 250,
                },
            ],
            "resultsSection.adverseEventsModule.seriousEvents": [
                {
                    "term": "Myocardial Infarction",
                    "organSystem": "Cardiac disorders",
                    "sourceVocabulary": "MedDRA",
                    "assessment": "systematic",
                    "notes": None,
                    "stats": [
                        {
                            "groupId": "EG000",
                            "numEvents": 2,
                            "numAffected": 2,
                            "numAtRisk": 250,
                        },
                    ],
                },
            ],
            "resultsSection.adverseEventsModule.otherEvents": [
                {
                    "term": "Headache",
                    "organSystem": "Nervous system disorders",
                    "sourceVocabulary": "MedDRA",
                    "assessment": "non-systematic",
                    "notes": "Mild to moderate",
                    "stats": [
                        {
                            "groupId": "EG000",
                            "numEvents": 15,
                            "numAffected": 12,
                            "numAtRisk": 250,
                        },
                    ],
                },
            ],
            # Annotations (Violations)
            "annotationSection.annotationModule.violationAnnotation.violationEvents": [
                {
                    "type": "LATE_RESULTS",
                    "description": "Results submitted 30 days late",
                    "issuedDate": "2026-01-15",
                    "creationDate": "2026-01-10",
                    "releaseDate": "2026-01-20",
                    "postedDate": "2026-01-25",
                },
            ],
        }
    )


@pytest.fixture
def browse_study_data():
    """Study data with browse modules (MeSH terms) populated."""
    return pd.Series(
        {
            # Conditions Browse Module
            "derivedSection.conditionBrowseModule.meshes": [
                {"id": "D003924", "term": "Diabetes Mellitus, Type 2"},
                {"id": "D006973", "term": "Hypertension"},
            ],
            "derivedSection.conditionBrowseModule.ancestors": [
                {"id": "D008659", "term": "Metabolic Diseases"},
            ],
            "derivedSection.conditionBrowseModule.browseLeaves": [
                {
                    "id": "M6694",
                    "name": "Diabetes Mellitus",
                    "asFound": "Diabetes",
                    "relevance": "HIGH",
                },
            ],
            "derivedSection.conditionBrowseModule.browseBranches": [
                {"abbrev": "BC18", "name": "Nutritional and Metabolic Diseases"},
            ],
            # Interventions Browse Module
            "derivedSection.interventionBrowseModule.meshes": [
                {"id": "D008687", "term": "Metformin"},
            ],
            "derivedSection.interventionBrowseModule.ancestors": [
                {"id": "D007004", "term": "Hypoglycemic Agents"},
            ],
            "derivedSection.interventionBrowseModule.browseLeaves": [
                {
                    "id": "M11366",
                    "name": "Metformin",
                    "asFound": "Metformin",
                    "relevance": "HIGH",
                },
            ],
            "derivedSection.interventionBrowseModule.browseBranches": [
                {
                    "abbrev": "BC19",
                    "name": "Antidiabetic Agents",
                }
            ],
        }
    )
