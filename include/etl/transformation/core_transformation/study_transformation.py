from typing import List, Dict
from collections import defaultdict
from datetime import date
import logging
import io

import pandas as pd
from config.env_config import config

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

s3_hook = S3Hook(aws_conn_id="aws_airflow")

from include.etl.transformation.config import SCALAR_FIELDS
from include.etl.transformation.utils import generate_key
from include.etl.transformation.models import StudyResult

log = logging.getLogger("airflow.task")

from include.etl.transformation.core_transformation.modules.study_main import (
    transform_scalar_fields,
)

from include.etl.transformation.core_transformation.modules.identification import (
    transform_identification_module,
)
from include.etl.transformation.core_transformation.modules.sponsor_collaborator import (
    transform_sponsor_and_collaborators_module,
)
from include.etl.transformation.core_transformation.modules.conditions import (
    transform_conditions_module,
)
from include.etl.transformation.core_transformation.modules.arms_intervention import (
    transform_arms_interventions_module,
)

from include.etl.transformation.core_transformation.modules.outcomes import (
    transform_outcomes_module,
)

from include.etl.transformation.core_transformation.modules.contacts_location import (
    transform_contacts_location_module,
)
from include.etl.transformation.core_transformation.modules.references import (
    transform_reference_module,
)
from include.etl.transformation.core_transformation.modules.outcome_measures import (
    transform_outcome_measures_module,
)

from include.etl.transformation.core_transformation.modules.participant_flow import (
    transform_participant_flow_module,
)

from include.etl.transformation.core_transformation.modules.adverse_events import (
    transform_adverse_events_module,
)

from include.etl.transformation.core_transformation.modules.annotations import (
    transform_annotations_module,
)

from include.etl.transformation.core_transformation.modules.conditions_browse import (
    transform_conditions_browse_module,
)

from include.etl.transformation.core_transformation.modules.interventions_browse import (
    transform_interventions_browse_module,
)


log = logging.getLogger("airflow.task")


def process_study_file(s3_key: str) -> List[StudyResult]:
    """
    Process a parquet file containing raw study data from ClinicalTrials.gov API.

    Reads the nested JSON structure, normalizes it, and transforms each study
    into the dimensional model schema. Studies missing NCT IDs are skipped
    with a warning.

    Args:
        s3_key: Path to parquet file containing raw API response data
                  with a 'studies' column of nested JSON.


    Returns:
        List of StudyResult objects, one per successfully processed study.

    Raises:
        Exception: Re-raises any transformation error after logging
    """
    batch_results: List[StudyResult] = []

    s3_uri = f"s3://{config.CLINEXA_BUCKET}/{s3_key}"
    log.info(f"READING {s3_uri}")

    s3_obj = s3_hook.get_key(key=s3_key, bucket_name=config.CLINEXA_BUCKET)
    file_content = s3_obj.get()["Body"].read()

    df_studies = pd.read_parquet(io.BytesIO(file_content))
    df_studies = pd.json_normalize(df_studies["studies"])

    for idx, study in df_studies.iterrows():
        nct_index = SCALAR_FIELDS["nct_id"]
        last_updated_index = SCALAR_FIELDS["last_updated"]

        last_updated = study[last_updated_index]
        nct_id = study.get(nct_index)

        if not nct_id:
            log.warning(f"Study missing NCT ID, skipping {idx}")
            continue

        try:
            result = transform_single_study(nct_id, study)
            batch_results.append(result)
        except Exception as e:
            log.exception(f"Error processing study {nct_id}: {e}")
            raise

    return batch_results


def transform_single_study(nct_id: str, study: pd.Series) -> StudyResult:
    """
    Transform a single study record into a normalised schema.

    Orchestrates all module-specific transformations and collects their outputs into a StudyResult.

    Args:
        nct_id: The ClinicalTrials.gov identifier (e.g., 'NCT12345678').
        study: Pandas Series containing the flattened study data from
               json_normalize, with dot-separated column names.

    Returns:
        StudyResult dataclass containing lists of records for each entity
        type in the model.
    """
    study_key = generate_key(nct_id)
    result = defaultdict(list)

    # scalar fields
    study_fields = transform_scalar_fields(study_key, study)
    result["studies"].append(study_fields)

    # identificationModule
    secondary_ids, nct_aliases = transform_identification_module(study_key, study)
    result["secondary_ids"].extend(secondary_ids)
    result["nct_aliases"].extend(nct_aliases)

    # sponsorCollaboratorsModule
    sponsor, study_sponsor, collaborators, study_collaborators = (
        transform_sponsor_and_collaborators_module(nct_id, study_key, study)
    )
    result["sponsors"].extend(sponsor)
    result["study_sponsors"].extend(study_sponsor)
    result["collaborators"].extend(collaborators)
    result["study_collaborators"].extend(study_collaborators)

    # conditionsModule
    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module(nct_id, study_key, study)
    )
    result["conditions"].extend(conditions)
    result["study_conditions"].extend(study_conditions)
    result["keywords"].extend(keywords)
    result["study_keywords"].extend(study_keywords)

    # armsInterventionsModule
    (
        arm_groups,
        arm_interventions,
        interventions,
        study_interventions,
        other_intervention_names,
        study_intervention_aliases,
    ) = transform_arms_interventions_module(study_key, study)
    result["arm_groups"].extend(arm_groups)
    result["arm_interventions"].extend(arm_interventions)
    result["interventions"].extend(interventions)
    result["study_interventions"].extend(study_interventions)
    result["other_intervention_names"].extend(other_intervention_names)
    result["study_intervention_aliases"].extend(study_intervention_aliases)

    # outcomesModule
    primary_outcomes, secondary_outcomes, other_outcomes = transform_outcomes_module(
        study_key, study
    )
    result["primary_outcomes"].extend(primary_outcomes)
    result["secondary_outcomes"].extend(secondary_outcomes)
    result["other_outcomes"].extend(other_outcomes)

    # contactsLocationsModule
    central_contacts, study_central_contacts, locations, study_locations = (
        transform_contacts_location_module(study_key, study)
    )
    result["central_contacts"].extend(central_contacts)
    result["study_central_contacts"].extend(study_central_contacts)
    result["locations"].extend(locations)
    result["study_locations"].extend(study_locations)

    # referencesModule
    study_references, link_references, ipd_references = transform_reference_module(
        study_key, study
    )
    result["study_references"].extend(study_references)
    result["link_references"].extend(link_references)
    result["ipd_references"].extend(ipd_references)

    # outcomeMeasuresModule
    (
        outcome_measures,
        outcome_measure_groups,
        outcome_measure_denom_units,
        outcome_measure_denom_counts,
        outcome_measure_groups_result,
        outcome_measure_analyses,
        outcome_measure_comparison_groups,
    ) = transform_outcome_measures_module(study_key, study)
    result["outcome_measures"].extend(outcome_measures)
    result["outcome_measure_groups"].extend(outcome_measure_groups)
    result["outcome_measure_denom_units"].extend(outcome_measure_denom_units)
    result["outcome_measure_denom_counts"].extend(outcome_measure_denom_counts)
    result["outcome_measure_groups_result"].extend(outcome_measure_groups_result)
    result["outcome_measure_analyses"].extend(outcome_measure_analyses)
    result["outcome_measure_comparison_groups"].extend(
        outcome_measure_comparison_groups
    )

    # participantFlowModule
    (
        flow_groups,
        flow_periods,
        flow_period_milestones,
        flow_period_milestone_achievements,
        withdrawal_types,
        flow_period_withdrawals,
        flow_period_withdrawal_reasons,
    ) = transform_participant_flow_module(study_key, study)
    result["flow_groups"].extend(flow_groups)
    result["flow_periods"].extend(flow_periods)
    result["flow_period_milestones"].extend(flow_period_milestones)
    result["flow_period_milestone_achievements"].extend(
        flow_period_milestone_achievements
    )
    result["withdrawal_types"].extend(withdrawal_types)
    result["df_flow_period_withdrawals"].extend(flow_period_withdrawals)
    result["flow_period_withdrawal_reasons"].extend(flow_period_withdrawal_reasons)

    # adverseEventsModule
    (
        adverse_events,
        event_groups,
        serious_events,
        serious_event_stats,
        other_events,
        other_event_stats,
    ) = transform_adverse_events_module(study_key, study)
    result["adverse_events"].extend(adverse_events)
    result["event_groups"].extend(event_groups)
    result["serious_events"].extend(serious_events)
    result["serious_event_stats"].extend(serious_event_stats)
    result["other_events"].extend(other_events)
    result["other_event_stats"].extend(other_event_stats)

    # annotationModule
    violations = transform_annotations_module(study_key, study)
    result["violations"].extend(violations)

    # conditionBrowseModule
    (
        conditions_mesh,
        study_conditions_mesh,
        conditions_mesh_ancestors,
        study_conditions_mesh_ancestors,
        conditions_browse_leaves,
        study_conditions_browse_leaves,
        conditions_browse_branches,
        study_conditions_browse_branches,
    ) = transform_conditions_browse_module(study_key, study)

    result["conditions_mesh"].extend(conditions_mesh)
    result["study_conditions_mesh"].extend(study_conditions_mesh)
    result["conditions_mesh_ancestors"].extend(conditions_mesh_ancestors)
    result["study_conditions_mesh_ancestors"].extend(study_conditions_mesh_ancestors)
    result["conditions_browse_leaves"].extend(conditions_browse_leaves)
    result["study_conditions_browse_leaves"].extend(study_conditions_browse_leaves)
    result["conditions_browse_branches"].extend(conditions_browse_branches)
    result["study_conditions_browse_branches"].extend(study_conditions_browse_branches)

    # interventionBrowseModule
    (
        interventions_mesh,
        study_interventions_mesh,
        interventions_mesh_ancestors,
        study_interventions_mesh_ancestors,
        interventions_browse_leaves,
        study_interventions_browse_leaves,
        interventions_browse_branches,
        study_interventions_browse_branches,
    ) = transform_interventions_browse_module(study_key, study)

    result["interventions_mesh"].extend(interventions_mesh)
    result["study_interventions_mesh"].extend(study_interventions_mesh)
    result["interventions_mesh_ancestors"].extend(interventions_mesh_ancestors)
    result["study_interventions_mesh_ancestors"].extend(
        study_interventions_mesh_ancestors
    )
    result["interventions_browse_leaves"].extend(interventions_browse_leaves)
    result["study_interventions_browse_leaves"].extend(
        study_interventions_browse_leaves
    )
    result["interventions_browse_branches"].extend(interventions_browse_branches)
    result["study_interventions_browse_branches"].extend(
        study_interventions_browse_branches
    )

    return StudyResult(
        studies=result["studies"],
        secondary_ids=result["secondary_ids"],
        nct_aliases=result["nct_aliases"],
        sponsors=result["sponsors"],
        study_sponsors=result["study_sponsors"],
        collaborators=result["collaborators"],
        study_collaborators=result["study_collaborators"],
        conditions=result["conditions"],
        study_conditions=result["study_conditions"],
        keywords=result["keywords"],
        study_keywords=result["study_keywords"],
        arm_groups=result["arm_groups"],
        arm_interventions=result["arm_interventions"],
        interventions=result["interventions"],
        study_interventions=result["study_interventions"],
        other_intervention_names=result["other_intervention_names"],
        study_intervention_aliases=result["study_intervention_aliases"],
        primary_outcomes=result["primary_outcomes"],
        secondary_outcomes=result["secondary_outcomes"],
        other_outcomes=result["other_outcomes"],
        central_contacts=result["central_contacts"],
        study_central_contacts=result["study_central_contacts"],
        locations=result["locations"],
        study_locations=result["study_locations"],
        study_references=result["study_references"],
        link_references=result["link_references"],
        ipd_references=result["ipd_references"],
        outcome_measures=result["outcome_measures"],
        outcome_measure_groups=result["outcome_measure_groups"],
        outcome_measure_denom_units=result["outcome_measure_denom_units"],
        outcome_measure_denom_counts=result["outcome_measure_denom_counts"],
        outcome_measure_groups_result=result["outcome_measure_groups_result"],
        outcome_measure_analyses=result["outcome_measure_analyses"],
        outcome_measure_comparison_groups=result["outcome_measure_comparison_groups"],
        flow_groups=result["flow_groups"],
        flow_periods=result["flow_periods"],
        flow_period_milestones=result["flow_period_milestones"],
        flow_period_milestone_achievements=result["flow_period_milestone_achievements"],
        withdrawal_types=result["withdrawal_types"],
        flow_period_withdrawals=result["df_flow_period_withdrawals"],
        flow_period_withdrawal_reasons=result["flow_period_withdrawal_reasons"],
        adverse_events=result["adverse_events"],
        event_groups=result["event_groups"],
        serious_events=result["serious_events"],
        serious_event_stats=result["serious_event_stats"],
        other_events=result["other_events"],
        other_event_stats=result["other_event_stats"],
        violations=result["violations"],
        conditions_mesh=result["conditions_mesh"],
        study_conditions_mesh=result["study_conditions_mesh"],
        conditions_mesh_ancestors=result["conditions_mesh_ancestors"],
        study_conditions_mesh_ancestors=result["study_conditions_mesh_ancestors"],
        conditions_browse_leaves=result["conditions_browse_leaves"],
        study_conditions_browse_leaves=result["study_conditions_browse_leaves"],
        conditions_browse_branches=result["conditions_browse_branches"],
        study_conditions_browse_branches=result["study_conditions_browse_branches"],
        interventions_mesh=result["interventions_mesh"],
        study_interventions_mesh=result["study_interventions_mesh"],
        interventions_mesh_ancestors=result["interventions_mesh_ancestors"],
        study_interventions_mesh_ancestors=result["study_interventions_mesh_ancestors"],
        interventions_browse_leaves=result["interventions_browse_leaves"],
        study_interventions_browse_leaves=result["study_interventions_browse_leaves"],
        interventions_browse_branches=result["interventions_browse_branches"],
        study_interventions_browse_branches=result[
            "study_interventions_browse_branches"
        ],
    )


def post_process_tables(results: Dict[str, List[Dict]]) -> Dict[str, pd.DataFrame]:
    """
    Convert raw transformation results into deduplicated DataFrames.

    Takes the aggregated dictionary of entity lists from batch processing,
    converts each to a DataFrame, and applies deduplication to dimension tables

    Args:
        results: Dictionary mapping entity names to lists of record dicts

     Returns:
        List of 64 DataFrames in fixed order matching the StudyResult schema,
        with dimension tables deduplicated on their primary keys. Order follows
        module grouping: studies, identification, sponsors, conditions,
        arms/interventions, outcomes, contacts/locations, references,
        outcome_measures, participant_flow, adverse_events, annotations,
        conditions_browse, interventions_browse.
    """

    df_studies = pd.DataFrame(results["studies"])

    # identificationModule
    df_secondary_ids = pd.DataFrame(results["secondary_ids"])
    df_nct_aliases = pd.DataFrame(results["nct_aliases"])

    # sponsorCollaboratorsModule
    df_sponsors = pd.DataFrame(results["sponsors"])
    df_study_sponsors = pd.DataFrame(results["study_sponsors"])
    df_collaborators = pd.DataFrame(results["collaborators"])
    df_study_collaborators = pd.DataFrame(results["study_collaborators"])

    # conditionsModule
    df_conditions = pd.DataFrame(results["conditions"])
    df_study_conditions = pd.DataFrame(results["study_conditions"])
    df_keywords = pd.DataFrame(results["keywords"])
    df_study_keywords = pd.DataFrame(results["study_keywords"])

    # armsInterventionsModule
    df_arm_groups = pd.DataFrame(results["arm_groups"])
    df_arm_interventions = pd.DataFrame(results["arm_interventions"])
    df_interventions = pd.DataFrame(results["interventions"])
    df_study_interventions = pd.DataFrame(results["study_interventions"])
    df_other_intervention_names = pd.DataFrame(results["other_intervention_names"])
    df_study_intervention_aliases = pd.DataFrame(results["study_intervention_aliases"])

    # outcomesModule
    df_primary_outcomes = pd.DataFrame(results["primary_outcomes"])
    df_secondary_outcomes = pd.DataFrame(results["secondary_outcomes"])
    df_other_outcomes = pd.DataFrame(results["other_outcomes"])

    # contactsLocationsModule
    df_central_contacts = pd.DataFrame(results["central_contacts"])
    df_study_central_contacts = pd.DataFrame(results["study_central_contacts"])
    df_locations = pd.DataFrame(results["locations"])
    df_study_locations = pd.DataFrame(results["study_locations"])

    # referencesModule
    df_study_references = pd.DataFrame(results["study_references"])
    df_link_references = pd.DataFrame(results["link_references"])
    df_ipd_references = pd.DataFrame(results["ipd_references"])

    # outcomeMeasuresModule
    df_outcome_measures = pd.DataFrame(results["outcome_measures"])
    df_outcome_measure_groups = pd.DataFrame(results["outcome_measure_groups"])
    df_outcome_measure_denom_units = pd.DataFrame(
        results["outcome_measure_denom_units"]
    )
    df_outcome_measure_denom_counts = pd.DataFrame(
        results["outcome_measure_denom_counts"]
    )
    df_outcome_measure_groups_result = pd.DataFrame(
        results["outcome_measure_groups_result"]
    )
    df_outcome_measure_analyses = pd.DataFrame(results["outcome_measure_analyses"])
    df_outcome_measure_comparison_groups = pd.DataFrame(
        results["outcome_measure_comparison_groups"]
    )

    # participantFlowModule
    df_flow_groups = pd.DataFrame(results["flow_groups"])
    df_flow_periods = pd.DataFrame(results["flow_periods"])
    df_flow_period_milestones = pd.DataFrame(results["flow_period_milestones"])
    df_flow_period_milestone_achievements = pd.DataFrame(
        results["flow_period_milestone_achievements"]
    )
    df_withdrawal_types = pd.DataFrame(results["withdrawal_types"])
    df_flow_period_withdrawals = pd.DataFrame(results["df_flow_period_withdrawals"])
    df_flow_period_withdrawal_reasons = pd.DataFrame(
        results["flow_period_withdrawal_reasons"]
    )

    # adverseEventsModule
    df_adverse_events = pd.DataFrame(results["adverse_events"])
    df_event_groups = pd.DataFrame(results["event_groups"])
    df_serious_events = pd.DataFrame(results["serious_events"])
    df_serious_event_stats = pd.DataFrame(results["serious_event_stats"])
    df_other_events = pd.DataFrame(results["other_events"])
    df_other_event_stats = pd.DataFrame(results["other_event_stats"])

    # annotationModule
    df_violations = pd.DataFrame(results["violations"])

    # conditionBrowseModule
    df_conditions_mesh = pd.DataFrame(results["conditions_mesh"])
    df_study_conditions_mesh = pd.DataFrame(results["study_conditions_mesh"])
    df_conditions_mesh_ancestors = pd.DataFrame(results["conditions_mesh_ancestors"])
    df_study_conditions_mesh_ancestors = pd.DataFrame(
        results["study_conditions_mesh_ancestors"]
    )
    df_conditions_browse_leaves = pd.DataFrame(results["conditions_browse_leaves"])
    df_study_conditions_browse_leaves = pd.DataFrame(
        results["study_conditions_browse_leaves"]
    )
    df_conditions_browse_branches = pd.DataFrame(results["conditions_browse_branches"])
    df_study_conditions_browse_branches = pd.DataFrame(
        results["study_conditions_browse_branches"]
    )

    # interventionBrowseModule
    df_interventions_mesh = pd.DataFrame(results["interventions_mesh"])
    df_study_interventions_mesh = pd.DataFrame(results["study_interventions_mesh"])
    df_interventions_mesh_ancestors = pd.DataFrame(
        results["interventions_mesh_ancestors"]
    )
    df_study_interventions_mesh_ancestors = pd.DataFrame(
        results["study_interventions_mesh_ancestors"]
    )
    df_interventions_browse_leaves = pd.DataFrame(
        results["interventions_browse_leaves"]
    )
    df_study_interventions_browse_leaves = pd.DataFrame(
        results["study_interventions_browse_leaves"]
    )
    df_interventions_browse_branches = pd.DataFrame(
        results["interventions_browse_branches"]
    )
    df_study_interventions_browse_branches = pd.DataFrame(
        results["study_interventions_browse_branches"]
    )

    # dedupe

    df_studies = df_studies.drop_duplicates(subset=["study_key"])
    df_secondary_ids = df_secondary_ids.drop_duplicates(subset=["secondary_id_key"])
    df_nct_aliases = df_nct_aliases.drop_duplicates(subset=["id_alias_key"])

    # sponsors amd collaborators
    df_sponsors = df_sponsors.drop_duplicates(subset=["sponsor_key"])
    df_study_sponsors = df_study_sponsors.drop_duplicates(
        subset=["study_key", "sponsor_key"]
    )
    df_collaborators = df_collaborators.drop_duplicates(subset=["collaborator_key"])
    df_study_collaborators = df_study_collaborators.drop_duplicates(
        subset=["study_key", "collaborator_key"]
    )

    # interventions
    df_interventions = df_interventions.drop_duplicates(subset=["intervention_key"])
    df_other_intervention_names = df_other_intervention_names.drop_duplicates(
        subset=["intervention_key"]
    )
    df_study_interventions = df_study_interventions.drop_duplicates(
        subset=["study_key", "intervention_key"]
    )
    df_study_intervention_aliases = df_study_intervention_aliases.drop_duplicates(
        subset=["study_key", "intervention_key"]
    )

    # conditions and keywords
    df_conditions = df_conditions.drop_duplicates(subset=["condition_key"])
    df_study_conditions = df_study_conditions.drop_duplicates(
        subset=["study_key", "condition_key"]
    )
    df_keywords = df_keywords.drop_duplicates(subset=["keyword_key"])
    df_study_keywords = df_study_keywords.drop_duplicates(
        subset=["study_key", "keyword_key"]
    )

    # locations and contacts
    df_central_contacts = df_central_contacts.drop_duplicates(subset=["contact_key"])
    df_study_central_contacts = df_study_central_contacts.drop_duplicates(
        subset=["contact_key", "study_key"]
    )
    df_locations = df_locations.drop_duplicates(subset=["location_key"])
    df_study_locations = df_study_locations.drop_duplicates(
        subset=["study_key", "location_key"]
    )

    # arm interventions
    df_arm_groups = df_arm_groups.drop_duplicates(subset=["study_key", "arm_group_key"])
    df_arm_interventions = df_arm_interventions.drop_duplicates(
        subset=["arm_intervention_key", "study_key", "arm_group_key"]
    )
    # outcomes
    df_primary_outcomes = df_primary_outcomes.drop_duplicates(subset=["outcome_key"])
    df_secondary_outcomes = df_secondary_outcomes.drop_duplicates(
        subset=["outcome_key"]
    )
    df_other_outcomes = df_other_outcomes.drop_duplicates(subset=["outcome_key"])

    df_study_references = df_study_references.drop_duplicates(subset=["ref_key"])
    df_link_references = df_link_references.drop_duplicates(subset=["link_key"])
    df_ipd_references = df_ipd_references.drop_duplicates(subset=["ipd_key"])

    # Outcome measures
    df_outcome_measures = df_outcome_measures.drop_duplicates(
        subset=["outcome_measure_key"]
    )
    df_outcome_measure_denom_units = df_outcome_measure_denom_units.drop_duplicates(
        subset=["denom_unit_key"]
    )
    df_outcome_measure_groups = df_outcome_measure_groups.drop_duplicates(
        subset=["group_key"]
    )
    df_outcome_measure_denom_counts = df_outcome_measure_denom_counts.drop_duplicates(
        subset=["denom_count_key"]
    )
    df_outcome_measure_groups_result = df_outcome_measure_groups_result.drop_duplicates(
        subset=["group_key", "outcome_measure_key", "study_key"]
    )
    df_outcome_measure_analyses = df_outcome_measure_analyses.drop_duplicates(
        subset=["analysis_key"]
    )
    df_outcome_measure_comparison_groups = (
        df_outcome_measure_comparison_groups.drop_duplicates(
            subset=["study_key", "outcome_measure_key", "analysis_key", "group_key"]
        )
    )

    # flow
    df_flow_groups = df_flow_groups.drop_duplicates(subset=["group_key"])
    df_flow_periods = df_flow_periods.drop_duplicates(subset=["period_key"])
    df_flow_period_milestones = df_flow_period_milestones.drop_duplicates(
        subset=["milestone_key"]
    )
    df_flow_period_milestone_achievements = (
        df_flow_period_milestone_achievements.drop_duplicates(
            subset=["study_key", "period_key", "milestone_key", "group_key"]
        )
    )
    df_withdrawal_types = df_withdrawal_types.drop_duplicates(
        subset=["withdrawal_type_key"]
    )
    df_flow_period_withdrawals = df_flow_period_withdrawals.drop_duplicates(
        subset=["study_key", "period_key", "withdrawal_type_key"]
    )
    df_flow_period_withdrawal_reasons = (
        df_flow_period_withdrawal_reasons.drop_duplicates(subset=["reason_key"])
    )

    # Adverse events
    df_adverse_events = df_adverse_events.drop_duplicates(subset=["adverse_event_key"])
    df_event_groups = df_event_groups.drop_duplicates(subset=["event_group_key"])
    df_serious_events = df_serious_events.drop_duplicates(subset=["serious_event_key"])
    df_serious_event_stats = df_serious_event_stats.drop_duplicates(
        subset=["event_stat_key"]
    )
    df_other_events = df_other_events.drop_duplicates(subset=["other_event_key"])
    df_other_event_stats = df_other_event_stats.drop_duplicates(
        subset=["event_stat_key"]
    )

    # conditions browse
    df_conditions_mesh = df_conditions_mesh.drop_duplicates(subset=["mesh_key"])
    df_study_conditions_mesh = df_study_conditions_mesh.drop_duplicates(
        subset=["mesh_key", "study_key"]
    )
    df_conditions_mesh_ancestors = df_conditions_mesh_ancestors.drop_duplicates(
        subset=["ancestor_key"]
    )
    df_study_conditions_mesh_ancestors = (
        df_study_conditions_mesh_ancestors.drop_duplicates(
            subset=["ancestor_key", "study_key"]
        )
    )
    df_conditions_browse_leaves = df_conditions_browse_leaves.drop_duplicates(
        subset=["leaf_key"]
    )
    df_study_conditions_browse_leaves = (
        df_study_conditions_browse_leaves.drop_duplicates(
            subset=["leaf_key", "study_key"]
        )
    )
    df_conditions_browse_branches = df_conditions_browse_branches.drop_duplicates(
        subset=["branch_key"]
    )
    df_study_conditions_browse_branches = (
        df_study_conditions_browse_branches.drop_duplicates(
            subset=["branch_key", "study_key"]
        )
    )
    # interventions browse
    df_interventions_mesh = df_interventions_mesh.drop_duplicates(subset=["mesh_key"])
    df_study_interventions_mesh = df_study_interventions_mesh.drop_duplicates(
        subset=["mesh_key", "study_key"]
    )
    df_interventions_mesh_ancestors = df_interventions_mesh_ancestors.drop_duplicates(
        subset=["ancestor_key"]
    )
    df_study_interventions_mesh_ancestors = (
        df_study_interventions_mesh_ancestors.drop_duplicates(
            subset=["ancestor_key", "study_key"]
        )
    )
    df_interventions_browse_leaves = df_interventions_browse_leaves.drop_duplicates(
        subset=["leaf_key"]
    )
    df_study_interventions_browse_leaves = (
        df_study_interventions_browse_leaves.drop_duplicates(
            subset=["leaf_key", "study_key"]
        )
    )
    df_interventions_browse_branches = df_interventions_browse_branches.drop_duplicates(
        subset=["branch_key"]
    )
    df_study_interventions_browse_branches = (
        df_study_interventions_browse_branches.drop_duplicates(
            subset=["branch_key", "study_key"]
        )
    )
    # Violations
    df_violations = df_violations.drop_duplicates(subset=["violation_key"])

    return {
        "studies": df_studies,
        "secondary_ids": df_secondary_ids,
        "nct_aliases": df_nct_aliases,
        "sponsors": df_sponsors,
        "study_sponsors": df_study_sponsors,
        "collaborators": df_collaborators,
        "study_collaborators": df_study_collaborators,
        "conditions": df_conditions,
        "study_conditions": df_study_conditions,
        "keywords": df_keywords,
        "study_keywords": df_study_keywords,
        "arm_groups": df_arm_groups,
        "arm_interventions": df_arm_interventions,
        "interventions": df_interventions,
        "study_interventions": df_study_interventions,
        "other_intervention_names": df_other_intervention_names,
        "study_intervention_aliases": df_study_intervention_aliases,
        "primary_outcomes": df_primary_outcomes,
        "secondary_outcomes": df_secondary_outcomes,
        "other_outcomes": df_other_outcomes,
        "central_contacts": df_central_contacts,
        "study_central_contacts": df_study_central_contacts,
        "locations": df_locations,
        "study_locations": df_study_locations,
        "study_references": df_study_references,
        "link_references": df_link_references,
        "ipd_references": df_ipd_references,
        "outcome_measures": df_outcome_measures,
        "outcome_measure_groups": df_outcome_measure_groups,
        "outcome_measure_denom_units": df_outcome_measure_denom_units,
        "outcome_measure_denom_counts": df_outcome_measure_denom_counts,
        "outcome_measure_groups_result": df_outcome_measure_groups_result,
        "outcome_measure_analyses": df_outcome_measure_analyses,
        "outcome_measure_comparison_groups": df_outcome_measure_comparison_groups,
        "flow_groups": df_flow_groups,
        "flow_periods": df_flow_periods,
        "flow_period_milestones": df_flow_period_milestones,
        "flow_period_milestone_achievements": df_flow_period_milestone_achievements,
        "withdrawal_types": df_withdrawal_types,
        "flow_period_withdrawals": df_flow_period_withdrawals,
        "flow_period_withdrawal_reasons": df_flow_period_withdrawal_reasons,
        "adverse_events": df_adverse_events,
        "event_groups": df_event_groups,
        "serious_events": df_serious_events,
        "serious_event_stats": df_serious_event_stats,
        "other_events": df_other_events,
        "other_event_stats": df_other_event_stats,
        "violations": df_violations,
        "conditions_mesh": df_conditions_mesh,
        "study_conditions_mesh": df_study_conditions_mesh,
        "conditions_mesh_ancestors": df_conditions_mesh_ancestors,
        "study_conditions_mesh_ancestors": df_study_conditions_mesh_ancestors,
        "conditions_browse_leaves": df_conditions_browse_leaves,
        "study_conditions_browse_leaves": df_study_conditions_browse_leaves,
        "conditions_browse_branches": df_conditions_browse_branches,
        "study_conditions_browse_branches": df_study_conditions_browse_branches,
        "interventions_mesh": df_interventions_mesh,
        "study_interventions_mesh": df_study_interventions_mesh,
        "interventions_mesh_ancestors": df_interventions_mesh_ancestors,
        "study_interventions_mesh_ancestors": df_study_interventions_mesh_ancestors,
        "interventions_browse_leaves": df_interventions_browse_leaves,
        "study_interventions_browse_leaves": df_study_interventions_browse_leaves,
        "interventions_browse_branches": df_interventions_browse_branches,
        "study_interventions_browse_branches": df_study_interventions_browse_branches,
    }
