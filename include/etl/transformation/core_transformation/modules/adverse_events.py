from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key


log = logging.getLogger("airflow.task")


def transform_adverse_events_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Transform adverse events data from nested JSON into normalised tables.

    Extracts adverse event information from a clinical trial study record and
    normalises it into six related tables: adverse event metadata, event groups
    (arms/cohorts), serious events, serious event statistics, other events,
    and other event statistics.

    Args:
        study_key: Surrogate key identifying the parent study record.
        study_data: Series containing the flattened study record with nested
            adverse events data accessible via the configured index field.

    Returns:
        A tuple of six lists, each containing dictionaries of transformed data

    Note:
        Returns empty lists for all outputs if adverse events data is missing
        or malformed.
    """

    adverse_events = []
    event_groups = []
    serious_events = []
    serious_event_stats = []
    other_events = []
    other_event_stats = []

    events_index = NON_SCALAR_FIELDS["adverse_events"]["index_field"]

    description = study_data.get(f"{events_index}.description")  # not always available
    adverse_event_key = generate_key(study_key, description)

    # scalar AE fields
    adverse_events.append(
        {
            "adverse_event_key": adverse_event_key,
            "study_key": study_key,
            "description": description,
            "frequency_threshold": study_data.get(f"{events_index}.frequencyThreshold"),
            "time_frame": study_data.get(f"{events_index}.timeFrame"),
            "mortality_cmt": study_data.get(f"{events_index}.allCauseMortalityComment"),
        }
    )

    event_group_list = study_data.get(f"{events_index}.eventGroups")

    if isinstance(event_group_list, (list, np.ndarray)) and len(event_group_list) > 0:
        for event_group in event_group_list:
            group_id = event_group.get("id")
            event_group_key = generate_key(study_key, adverse_event_key, group_id)

            event_groups.append(
                {
                    "event_group_key": event_group_key,
                    "study_key": study_key,
                    "adverse_event_key": adverse_event_key,
                    "group_id": group_id,
                    "title": event_group.get("title"),
                    "description": event_group.get("description"),
                    "num_deaths": event_group.get("deathsNumAffected"),
                    "num_deaths_at_risk": event_group.get("deathsNumAtRisk"),
                    "num_serious": event_group.get("seriousNumAffected"),
                    "num_serious_at_risk": event_group.get("seriousNumAtRisk"),
                    "num_other": event_group.get("otherNumAffected"),
                    "num_other_at_risk": event_group.get("otherNumAtRisk"),
                }
            )
    # serious AEs
    serious_events_list = study_data.get(f"{events_index}.seriousEvents")

    if (
        isinstance(serious_events_list, (list, np.ndarray))
        and len(serious_events_list) > 0
    ):
        for serious_event in serious_events_list:
            term = serious_event.get("term").lower()
            serious_event_key = generate_key(study_key, adverse_event_key, term)

            organ_system = serious_event.get("organSystem")
            organ_system = organ_system.lower() if organ_system else None

            assessment_type = serious_event.get("assessment")
            assessment_type = assessment_type.upper() if assessment_type else None

            serious_events.append(
                {
                    "serious_event_key": serious_event_key,
                    "adverse_event_key": adverse_event_key,
                    "study_key": study_key,
                    "term": term,
                    "organ_system": organ_system,
                    "source_vocab": serious_event.get("sourceVocabulary"),
                    "assessment_type": assessment_type,
                    "notes": serious_event.get("notes"),
                }
            )

            serious_event_stats_list = serious_event.get("stats")
            if (
                isinstance(serious_event_stats_list, (list, np.ndarray))
                and len(serious_event_stats_list) > 0
            ):
                for serious_event_stat in serious_event_stats_list:
                    group_id = serious_event_stat.get("groupId")

                    group_key = generate_key(study_key, adverse_event_key, group_id)
                    event_stat_key = generate_key(
                        study_key, adverse_event_key, serious_event_key, group_id
                    )

                    serious_event_stats.append(
                        {
                            "event_stat_key": event_stat_key,
                            "adverse_event_key": adverse_event_key,
                            "serious_event_key": serious_event_key,
                            "study_key": study_key,
                            "group_key": group_key,
                            "group_id": group_id,
                            "num_events": serious_event_stat.get("numEvents"),
                            "num_affected": serious_event_stat.get("numAffected"),
                            "num_at_risk": serious_event_stat.get("numAtRisk"),
                        }
                    )

    # other AEs
    other_events_list = study_data.get(f"{events_index}.otherEvents")
    if isinstance(other_events_list, (list, np.ndarray)) and len(other_events_list) > 0:
        for other_event in other_events_list:

            term = other_event.get("term")
            other_event_key = generate_key(study_key, adverse_event_key, term)

            organ_system = other_event.get("organSystem")
            organ_system = organ_system.lower() if organ_system else None

            assessment_type = other_event.get("assessment")
            assessment_type = assessment_type.upper() if assessment_type else None

            other_events.append(
                {
                    "other_event_key": other_event_key,
                    "adverse_event_key": adverse_event_key,
                    "study_key": study_key,
                    "term": term,
                    "organ_system": organ_system,
                    "source_vocab": other_event.get("sourceVocabulary"),
                    "assessment_type": assessment_type,
                    "notes": other_event.get("notes"),
                }
            )

            other_event_stats_list = other_event.get("stats")
            if (
                isinstance(other_event_stats_list, (list, np.ndarray))
                and len(other_event_stats_list) > 0
            ):
                for other_event_stat in other_event_stats_list:
                    group_id = other_event_stat.get("groupId")

                    group_key = generate_key(study_key, adverse_event_key, group_id)
                    event_stat_key = generate_key(
                        study_key, adverse_event_key, other_event_key, group_id
                    )

                    other_event_stats.append(
                        {
                            "event_stat_key": event_stat_key,
                            "other_event_key": other_event_key,
                            "adverse_event_key": adverse_event_key,
                            "study_key": study_key,
                            "group_key": group_key,
                            "group_id": group_id,
                            "num_events": other_event_stat.get("numEvents"),
                            "num_affected": other_event_stat.get("numAffected"),
                            "num_at_risk": other_event_stat.get("numAtRisk"),
                        }
                    )

    return (
        adverse_events,
        event_groups,
        serious_events,
        serious_event_stats,
        other_events,
        other_event_stats,
    )
