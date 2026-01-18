from include.etl.transformation.core_transformation.modules.conditions import (
    transform_conditions_module,
)
from tests.unit.transformation.fixtures import (
    study_key,
    full_study_data,
    empty_study_data,
    nct_id,
)


def test_empty_data_returns_empty_lists(nct_id, study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module(nct_id, study_key, empty_study_data)
    )

    assert conditions == []
    assert study_conditions == []
    assert keywords == []
    assert study_keywords == []


def test_conditions_extracted_lowercase(nct_id, study_key, full_study_data):
    """Test conditions are extracted and lowercased."""
    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module(nct_id, study_key, full_study_data)
    )

    assert len(conditions) == 2
    assert conditions[0]["condition_name"] == "type 2 diabetes"
    assert conditions[1]["condition_name"] == "hypertension"


def test_keywords_extracted_lowercase(nct_id, study_key, full_study_data):
    """Test keywords are extracted and lowercased."""
    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module(nct_id, study_key, full_study_data)
    )

    assert len(keywords) == 3
    keyword_names = [k["keyword_name"] for k in keywords]
    assert "diabetes" in keyword_names
    assert "blood pressure" in keyword_names


def test_bridge_tables_populated(nct_id, study_key, full_study_data):
    """Test bridge tables are populated with keys."""
    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module(nct_id, study_key, full_study_data)
    )

    assert len(study_conditions) == 2
    assert all(sc["study_key"] == study_key for sc in study_conditions)
    assert len(study_keywords) == 3
