from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_participant_flow_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Transform participant flow data from a completed clinical trial.

    Participant flow tracks how subjects moved through the study.
    This data is only available for studies that have posted results.

    The module extracts a hierarchical structure:
    - Groups: Treatment/control arms for flow reporting
    - Periods: Study phases (e.g., screening, treatment, follow-up)
    - Milestones: Key transition points within periods (started, completed)
    - Achievements: Per-group counts at each milestone
    - Withdrawals: Dropout/discontinuation events per period
    - Withdrawal reasons: Specific reasons with per-group counts

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested participant flow
            data at the path specified in NON_SCALAR_FIELDS["participant_flow"].

    Returns:
        Six-element tuple containing:
            - flow_groups: Group definitions
            - flow_periods: Period definitions
            - flow_period_milestones: Milestone definitions per period
            - flow_period_milestone_achievements: Per-group counts at milestones
            - flow_period_withdrawals: Withdrawal event definitions per period
            - flow_period_withdrawal_reasons: Per-group withdrawal reasons/counts

        All lists return empty if no participant flow data exists.

    """
    flow_groups = []
    flow_periods = []
    flow_period_milestones = []
    flow_period_milestone_achievements = []
    withdrawal_types = []
    flow_period_withdrawals = []
    flow_period_withdrawal_reasons = []

    flow_module_index = NON_SCALAR_FIELDS["participant_flow"]["index_field"]

    # flow groups
    flow_group_list = study_data.get(f"{flow_module_index}.groups")
    if isinstance(flow_group_list, (list, np.ndarray)) and len(flow_group_list) > 0:
        for flow in flow_group_list:
            group_id = flow.get("id")
            group_key = generate_key(study_key, group_id)

            flow_groups.append(
                {
                    "study_key": study_key,
                    "group_key": group_key,
                    "id": group_id,
                    "title": flow.get("title"),
                    "description": flow.get("description"),
                }
            )

    # flow periods

    flow_period_list = study_data.get(f"{flow_module_index}.periods")
    if isinstance(flow_period_list, (list, np.ndarray)) and len(flow_period_list) > 0:
        for period in flow_period_list:
            period_title = period.get("title")
            period_key = generate_key(study_key, period_title)

            # flow periods
            flow_periods.append(
                {
                    "study_key": study_key,
                    "period_key": period_key,
                    "title": period.get("title"),
                }
            )

            # flow milestones
            period_milestones = period.get("milestones")
            if (
                isinstance(period_milestones, (list, np.ndarray))
                and len(period_milestones) > 0
            ):
                for period_milestone in period_milestones:
                    milestone_type = period_milestone.get("type")
                    milestone_comment = period_milestone.get("comment")
                    milestone_key = generate_key(
                        study_key, period_key, milestone_type, milestone_comment
                    )

                    flow_period_milestones.append(
                        {
                            "study_key": study_key,
                            "period_key": period_key,
                            "milestone_key": milestone_key,
                            "type": milestone_type,
                            "comment": milestone_comment,
                        }
                    )

                    # milestone achievements
                    milestone_achievements = period_milestone.get("achievements")
                    if (
                        isinstance(milestone_achievements, (list, np.ndarray))
                        and len(milestone_achievements) > 0
                    ):
                        for achievement in milestone_achievements:
                            group_id = achievement.get("groupId")
                            group_key = generate_key(study_key, group_id)
                            flow_period_milestone_achievements.append(
                                {
                                    "study_key": study_key,
                                    "period_key": period_key,
                                    "milestone_key": milestone_key,
                                    "group_key": group_key,
                                    "group_id": group_id,
                                    "comment": achievement.get("comment"),
                                    "num_subjects": achievement.get("numSubjects"),
                                    "num_units": achievement.get("numUnits"),
                                }
                            )

            period_withdrawals = period.get("dropWithdraws")
            if (
                isinstance(period_withdrawals, (list, np.ndarray))
                and len(period_withdrawals) > 0
            ):
                for withdrawal in period_withdrawals:
                    withdrawal_type = withdrawal.get("type")
                    withdrawal_type_key = generate_key(withdrawal_type)

                    withdrawal_types.append(
                        {
                            "withdrawal_type_key": withdrawal_type_key,
                            "type": withdrawal_type,
                        }
                    )

                    flow_period_withdrawals.append(
                        {
                            "study_key": study_key,
                            "period_key": period_key,
                            "withdrawal_type_key": withdrawal_type_key,
                            "comment": withdrawal.get("comment"),
                        }
                    )

                    # withdrawal reasons
                    period_withdrawal_reasons = withdrawal.get("reasons")

                    if (
                        isinstance(period_withdrawal_reasons, (list, np.ndarray))
                        and len(period_withdrawal_reasons) > 0
                    ):
                        for reason in period_withdrawal_reasons:

                            reason_group_id = reason.get("groupId")
                            reason_comment = reason.get("comment")

                            reason_key = generate_key(
                                study_key,
                                period_key,
                                withdrawal_type_key,
                                reason_group_id,
                                reason_comment,
                            )

                            group_key = generate_key(study_key, reason_group_id)

                            flow_period_withdrawal_reasons.append(
                                {
                                    "study_key": study_key,
                                    "period_key": period_key,
                                    "withdrawal_type_key": withdrawal_type_key,
                                    "reason_key": reason_key,
                                    "group_id": reason_group_id,
                                    "group_key": group_key,
                                    "reason": reason_comment,
                                    "num_subjects": reason.get("numSubjects"),
                                }
                            )

    return (
        flow_groups,
        flow_periods,
        flow_period_milestones,
        flow_period_milestone_achievements,
        withdrawal_types,
        flow_period_withdrawals,
        flow_period_withdrawal_reasons,
    )
