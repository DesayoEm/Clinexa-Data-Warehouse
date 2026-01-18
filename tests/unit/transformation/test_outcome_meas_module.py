from include.etl.transformation.core_transformation.modules.outcome_measures import (
    transform_outcome_measures_module,
)
from tests.unit.transformation.fixtures import (
    study_key,
    results_study_data,
    empty_study_data,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    result = transform_outcome_measures_module(study_key, empty_study_data)

    assert all(lst == [] for lst in result)


def test_outcome_measures_extracted(study_key, results_study_data):
    """Test outcome measure definitions are extracted."""
    outcome_measures, *_ = transform_outcome_measures_module(
        study_key, results_study_data
    )

    assert len(outcome_measures) == 1
    assert outcome_measures[0]["title"] == "HbA1c Change from Baseline"
    assert outcome_measures[0]["outcome_type"] == "PRIMARY"
    assert outcome_measures[0]["param_type"] == "MEAN"


def test_groups_extracted(study_key, results_study_data):
    """Test outcome measure groups are extracted."""
    _, groups, *_ = transform_outcome_measures_module(study_key, results_study_data)

    assert len(groups) == 2
    group_ids = [g["group_id"] for g in groups]
    assert "OG000" in group_ids
    assert "OG001" in group_ids


def test_denom_counts_extracted(study_key, results_study_data):
    """Test denominator counts are extracted."""
    _, _, denom_units, denom_counts, *_ = transform_outcome_measures_module(
        study_key, results_study_data
    )

    assert len(denom_units) == 1
    assert denom_units[0]["denom_unit"] == "PARTICIPANTS"
    assert len(denom_counts) == 2


def test_measurements_extracted(study_key, results_study_data):
    """Test measurements are extracted."""
    _, _, _, _, measurements, *_ = transform_outcome_measures_module(
        study_key, results_study_data
    )

    assert len(measurements) == 2
    values = [m["value"] for m in measurements]
    assert "-1.2" in values
    assert "-0.3" in values


def test_analyses_extracted(study_key, results_study_data):
    """Statistical analyses are extracted."""

    *_, analyses, comparison_groups = transform_outcome_measures_module(
        study_key, results_study_data
    )

    assert len(analyses) == 1
    assert analyses[0]["p_value"] == "0.001"
    assert len(comparison_groups) == 2
