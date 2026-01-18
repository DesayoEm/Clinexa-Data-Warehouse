from include.etl.transformation.core_transformation.modules.sponsor_collaborator import (
    transform_sponsor_and_collaborators_module,
)


def test_empty_data_returns_empty_lists(nct_id, study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    sponsor, study_sponsor, collaborators, study_collaborators = (
        transform_sponsor_and_collaborators_module(nct_id, study_key, empty_study_data)
    )

    assert sponsor == []
    assert study_sponsor == []
    assert collaborators == []
    assert study_collaborators == []


def test_lead_sponsor_extracted(nct_id, study_key, full_study_data):
    """Test lead sponsor is properly extracted."""
    sponsor, study_sponsor, collaborators, study_collaborators = (
        transform_sponsor_and_collaborators_module(nct_id, study_key, full_study_data)
    )

    assert len(sponsor) == 1
    assert sponsor[0]["name"] == "Test Pharma Inc"
    assert sponsor[0]["sponsor_class"] == "INDUSTRY"
    assert len(study_sponsor) == 1
    assert study_sponsor[0]["study_key"] == study_key


def test_collaborators_extracted(nct_id, study_key, full_study_data):
    """Test collaborators are properly extracted."""
    sponsor, study_sponsor, collaborators, study_collaborators = (
        transform_sponsor_and_collaborators_module(nct_id, study_key, full_study_data)
    )

    assert len(collaborators) == 2
    assert collaborators[0]["name"] == "University Hospital"
    assert collaborators[0]["collaborator_class"] == "OTHER"
    assert len(study_collaborators) == 2
