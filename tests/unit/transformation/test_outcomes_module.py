from include.etl.transformation.core_transformation.modules.outcomes import (
    transform_outcomes_module,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Empty study data returns empty lists."""
    primary, secondary, other = transform_outcomes_module(study_key, empty_study_data)

    assert primary == []
    assert secondary == []
    assert other == []


def test_primary_outcomes_extracted(study_key, full_study_data):
    """Test primary outcomes are extracted."""
    primary, secondary, other = transform_outcomes_module(study_key, full_study_data)

    assert len(primary) == 1
    assert primary[0]["measure"] == "HbA1c Change"
    assert primary[0]["time_frame"] == "12 weeks"
    assert primary[0]["study_key"] == study_key


def test_secondary_outcomes_extracted(study_key, full_study_data):
    """Test secondary outcomes are properly extracted."""
    primary, secondary, other = transform_outcomes_module(study_key, full_study_data)

    assert len(secondary) == 1
    assert secondary[0]["measure"] == "Blood Pressure"


def test_other_outcomes_extracted(study_key, full_study_data):
    """Test other outcomes are properly extracted."""
    primary, secondary, other = transform_outcomes_module(study_key, full_study_data)

    assert len(other) == 1
    assert other[0]["measure"] == "Quality of Life"
