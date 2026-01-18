
from include.etl.transformation.core_transformation.study_transformation import transform_scalar_fields
from tests.unit.transformation.fixtures import study_key, minimal_study_data, full_study_data


def test_transform_scalar_fields_minimal(study_key, minimal_study_data):
    """Test minimal data produces record with study_key and available fields."""
    result = transform_scalar_fields(study_key, minimal_study_data)
    
    assert result["study_key"] == study_key
    assert result["nct_id"] == "NCT12345678"
    assert result["brief_title"] == "Test Study"

def test_transform_scalar_fields_missing_fields_are_none(study_key, minimal_study_data):
    """Test missing fields return None values."""
    
    result = transform_scalar_fields(study_key, minimal_study_data)

    assert result["official_title"] is None
    assert result["brief_summary"] is None


def test_transform_scalar_fields_all_fields(study_key, full_study_data):
    """Test full data produces complete record."""

    result = transform_scalar_fields(study_key, full_study_data)

    assert result["nct_id"] == "NCT12345678"
    assert result["brief_title"] == "Test Clinical Trial"
    assert result["official_title"] == "A Phase 3 Randomized Controlled Trial"
    assert result["acronym"] == "TCT"
    assert result["overall_status"] == "RECRUITING"
    assert result["study_type"] == "INTERVENTIONAL"
    assert result["enrollment_count"] == 500
    assert result["has_results"] == True