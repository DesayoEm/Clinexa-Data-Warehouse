from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_outcomes_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Transform planned outcome definitions from a clinical trial protocol.

    Distinct from transform_outcome_measures_module, which contains actual
    results. This module captures the pre-specified endpoints defined in the
    study protocol before results are available

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested outcome definitions
            at the path specified in NON_SCALAR_FIELDS["outcomes"].

    Returns:
        Three-element tuple containing:
            - primary_outcomes: Primary endpoint definitions
            - secondary_outcomes: Secondary endpoint definitions
            - other_outcomes: Other endpoint definitions

        All lists return empty if no outcome definitions exist for the study.
    """
    primary_outcomes = []
    secondary_outcomes = []
    other_outcomes = []

    outcomes_module_index = NON_SCALAR_FIELDS["outcomes"]["index_field"]

    primary_outcomes_list = study_data.get(f"{outcomes_module_index}.primaryOutcomes")

    if (
        isinstance(primary_outcomes_list, (list, np.ndarray))
        and len(primary_outcomes_list) > 0
    ):
        for primary_outcome in primary_outcomes_list:
            outcome_measure = primary_outcome.get("measure")
            outcome_description = primary_outcome.get("description")
            outcome_timeframe = primary_outcome.get("timeFrame")

            outcome_key = generate_key(study_key, outcome_measure, outcome_description, outcome_timeframe)

            primary_outcomes.append(
                {
                    "study_key": study_key,
                    "outcome_key": outcome_key,
                    "measure": outcome_measure,
                    "description": outcome_description,
                    "time_frame": outcome_timeframe,
                }
            )

    secondary_outcomes_list = study_data.get(
        f"{outcomes_module_index}.secondaryOutcomes"
    )
    if (
        isinstance(secondary_outcomes_list, (list, np.ndarray))
        and len(secondary_outcomes_list) > 0
    ):
        for secondary_outcome in secondary_outcomes_list:
            outcome_measure = secondary_outcome.get("measure")
            outcome_description = secondary_outcome.get("description")
            outcome_timeframe = secondary_outcome.get("timeFrame")

            outcome_key = generate_key(study_key, outcome_measure, outcome_description, outcome_timeframe)

            secondary_outcomes.append(
                {
                    "study_key": study_key,
                    "outcome_key": outcome_key,
                    "measure": outcome_measure,
                    "description": outcome_description,
                    "time_frame": outcome_timeframe,
                }
            )


    other_outcomes_list = study_data.get(f"{outcomes_module_index}.otherOutcomes")
    if (
        isinstance(other_outcomes_list, (list, np.ndarray))
        and len(other_outcomes_list) > 0
    ):
        for other_outcome in other_outcomes_list:
            outcome_measure = other_outcome.get("measure")
            outcome_description = other_outcome.get("description")
            outcome_timeframe = other_outcome.get("timeFrame")

            outcome_key = generate_key(study_key, outcome_measure, outcome_description, outcome_timeframe)

            other_outcomes.append(
                {
                    "study_key": study_key,
                    "outcome_key": outcome_key,
                    "measure": outcome_measure,
                    "description": outcome_description,
                    "time_frame": outcome_timeframe,
                }
            )

    return primary_outcomes, secondary_outcomes, other_outcomes
