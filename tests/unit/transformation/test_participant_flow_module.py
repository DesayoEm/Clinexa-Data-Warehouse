from include.etl.transformation.core_transformation.modules.participant_flow import (
    transform_participant_flow_module,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    result = transform_participant_flow_module(study_key, empty_study_data)
    assert all(lst == [] for lst in result)


def test_flow_groups_extracted(study_key, results_study_data):
    """Test flow groups are extracted."""
    groups, *_ = transform_participant_flow_module(study_key, results_study_data)

    assert len(groups) == 2
    assert groups[0]["title"] == "Treatment Arm"


def test_periods_extracted(study_key, results_study_data):
    """Test flow periods are extracted."""

    _, periods, *_ = transform_participant_flow_module(study_key, results_study_data)

    assert len(periods) == 1
    assert periods[0]["title"] == "Screening"


def test_milestones_and_achievements_extracted(study_key, results_study_data):
    """Test milestones and achievements are extracted."""
    _, _, milestones, achievements, *_ = transform_participant_flow_module(
        study_key, results_study_data
    )

    assert len(milestones) == 1
    assert milestones[0]["type"] == "STARTED"
    assert len(achievements) == 2


def test_withdrawals_extracted(study_key, results_study_data):
    """Test withdrawals and reasons are properly extracted."""

    *_, withdrawal_types, flow_period_withdrawals, flow_period_withdrawal_reasons = (
        transform_participant_flow_module(study_key, results_study_data)
    )

    assert len(flow_period_withdrawals) == 1
    assert withdrawal_types[0]["type"] == "Withdrawal"
    assert len(flow_period_withdrawal_reasons) == 1
