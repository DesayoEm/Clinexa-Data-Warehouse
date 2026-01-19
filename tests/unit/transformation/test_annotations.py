from include.etl.transformation.core_transformation.modules.annotations import (
    transform_annotations_module,
)


def test_empty_data_returns_empty_list(study_key, empty_study_data):
    """Empty study data returns empty list."""
    violations = transform_annotations_module(study_key, empty_study_data)

    assert violations == []


def test_violations_extracted(study_key, results_study_data):
    """FDAAA 801 violations are properly extracted."""
    violations = transform_annotations_module(study_key, results_study_data)

    assert len(violations) == 1
    assert violations[0]["violation_type"] == "LATE_RESULTS"
    assert violations[0]["issued_date"] == "2026-01-15"
    assert violations[0]["study_key"] == study_key
