from dataclasses import dataclass, asdict, fields
from typing import List, Dict


@dataclass
class StudyResult:
    """
    Container for all dimensional model records extracted from a single study.

    Groups the output of transform_single_study into named attributes for
    clarity when aggregating batch results. Each attribute holds a list of dicts representing records
    for that entity table.
    """

    studies: List  # single study
    secondary_ids: List
    nct_aliases: List
    sponsors: List
    study_sponsors: List
    collaborators: List
    study_collaborators: List
    conditions: List
    study_conditions: List
    keywords: List
    study_keywords: List
    arm_groups: List
    arm_interventions: List
    intervention_names: List
    study_interventions: List
    other_intervention_names: List
    study_intervention_aliases: List
    primary_outcomes: List
    secondary_outcomes: List
    other_outcomes: List
    central_contacts: List
    study_central_contacts: List
    locations: List
    study_locations: List
    references: List
    link_references: List
    ipd_references: List
    outcome_measures: List
    outcome_measure_groups: List
    outcome_measure_denom_units: List
    outcome_measure_denom_counts: List
    outcome_measure_measurements: List
    outcome_measure_analyses: List
    outcome_measure_comparison_groups: List
    flow_groups: List
    flow_periods: List
    flow_period_milestones: List
    flow_period_milestone_achievements: List
    df_flow_period_withdrawals: List
    flow_period_withdrawal_reasons: List
    adverse_events: List
    event_groups: List
    serious_events: List
    serious_event_stats: List
    other_events: List
    other_event_stats: List
    violations: List
    conditions_mesh: List
    study_conditions_mesh: List
    conditions_mesh_ancestors: List
    study_conditions_mesh_ancestors: List
    conditions_browse_leaves: List
    study_conditions_browse_leaves: List
    conditions_browse_branches: List
    study_conditions_browse_branches: List
    interventions_mesh: List
    study_interventions_mesh: List
    interventions_mesh_ancestors: List
    study_interventions_mesh_ancestors: List
    interventions_browse_leaves: List
    study_interventions_browse_leaves: List
    interventions_browse_branches: List
    study_interventions_browse_branches: List

    def tables(self) -> Dict[str, List[Dict]]:
        return asdict(self)

    @classmethod
    def expected_tables(cls) -> List[str]:
        """Canonical list of expected output tables for the StudyResult schema."""
        return [f.name for f in fields(cls)]
