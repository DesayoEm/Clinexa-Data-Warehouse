from include.etl.transformation.core_transformation.modules.adverse_events import (
    transform_adverse_events_module,
)
from tests.unit.transformation.fixtures import (
    study_key,
    results_study_data,
    empty_study_data,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns lists with one AE record (always created)."""

    ae, groups, serious, serious_stats, other, other_stats = (
        transform_adverse_events_module(study_key, empty_study_data)
    )

    assert len(ae) == 1
    assert groups == []
    assert serious == []
    assert other == []


def test_adverse_events_metadata_extracted(study_key, results_study_data):
    """Test adverse events metadata is properly extracted."""
    ae, *_ = transform_adverse_events_module(study_key, results_study_data)

    assert len(ae) == 1
    assert ae[0]["description"] == "Safety analysis population"
    assert ae[0]["frequency_threshold"] == "5"
    assert ae[0]["time_frame"] == "12 weeks"


def test_event_groups_extracted(study_key, results_study_data):
    """Test event groups are properly extracted."""
    _, groups, *_ = transform_adverse_events_module(study_key, results_study_data)

    assert len(groups) == 1
    assert groups[0]["title"] == "Treatment"
    assert groups[0]["num_serious"] == 5
    assert groups[0]["num_serious_at_risk"] == 250


def test_serious_events_extracted_lowercase(study_key, results_study_data):
    """Test serious events are extracted with lowercase term."""
    _, _, serious, serious_stats, *_ = transform_adverse_events_module(
        study_key, results_study_data
    )

    assert len(serious) == 1
    assert serious[0]["term"] == "myocardial infarction"
    assert serious[0]["organ_system"] == "cardiac disorders"
    assert serious[0]["assessment_type"] == "SYSTEMATIC"


def test_serious_event_stats_extracted(study_key, results_study_data):
    """Test serious event statistics are properly extracted."""
    _, _, _, serious_stats, *_ = transform_adverse_events_module(
        study_key, results_study_data
    )

    assert len(serious_stats) == 1
    assert serious_stats[0]["num_events"] == 2
    assert serious_stats[0]["num_affected"] == 2


def test_other_events_extracted(study_key, results_study_data):
    """Test other (non-serious) events are properly extracted."""

    *_, other, other_stats = transform_adverse_events_module(
        study_key, results_study_data
    )

    assert len(other) == 1
    assert other[0]["term"] == "Headache"  # Not lowercased in other_events
    assert len(other_stats) == 1
