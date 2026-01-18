from include.etl.transformation.core_transformation.modules.identification import (
    transform_identification_module,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    secondary_ids, nct_aliases = transform_identification_module(
        study_key, empty_study_data
    )

    assert secondary_ids == []
    assert nct_aliases == []


def test_secondary_ids_extracted(study_key, full_study_data):
    """Test secondary IDs are extracted."""
    secondary_ids, nct_aliases = transform_identification_module(
        study_key, full_study_data
    )

    assert len(secondary_ids) == 2
    assert secondary_ids[0]["id"] == "SEC-001"
    assert secondary_ids[0]["type"] == "OTHER"
    assert secondary_ids[0]["domain"] == "NIH"
    assert secondary_ids[0]["study_key"] == study_key


def test_nct_aliases_extracted(study_key, full_study_data):
    """Test NCT aliases are extracted."""
    secondary_ids, nct_aliases = transform_identification_module(
        study_key, full_study_data
    )

    assert len(nct_aliases) == 2
    assert nct_aliases[0]["id_alias"] == "NCT00000001"
    assert nct_aliases[0]["study_key"] == study_key
