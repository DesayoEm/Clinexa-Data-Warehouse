from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


ALIASES = {
    "sham": "Placebo",
    "5-fu": "Fluorouracil",
    "5-fluorouracil": "Fluorouracil",
    "trastuzumab emtansine": "Trastuzumab",
    "t-dm1": "Trastuzumab",
}


ARMS_SOURCE = "ARM"


def standardize_intervention_name(
    intervention_name: str, source: str | None = None
) -> str | None:
    # Manual cleaning is not enough to handle randomness and unpredictability of the intervention names
    # as they're entered manually without validation on the API therefore:
    # MeSH terms are more reliable for queries but intervention names takes precedence over mesh during
    # API search as they're more specific and layman friendly

    if not intervention_name:
        return None

    if source == ARMS_SOURCE:
        # armGroups[].interventionNames uses format "Type: Name" (e.g., "Drug: Cisplatin")
        # interventions[].name uses just "Name" (e.g., "Cisplatin")
        # Stripping prefix is necessary to enable joining arm_interventions -> interventions

        parts = intervention_name.split(": ", 1)
        intervention_name = parts[1] if len(parts) > 1 else intervention_name

    cleaned = intervention_name.lower().strip()
    if cleaned in ALIASES:
        return ALIASES[cleaned]

    if cleaned.startswith("placebo"):
        # extensive descriptions of placebo arms e.g 'Placebo matched to M2951'
        # should only be in the description field in arms_intervention list
        return "Placebo"

    words = intervention_name.strip().split()
    return " ".join([w if w.isupper() else w.capitalize() for w in words])


def transform_arms_interventions_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Extract arm groups and interventions from a clinical trial study.

    Processes the arms/interventions module to produce normalized tables for:
    - Arm groups (treatment/control cohorts in the study design)
    - Arm-to-intervention mappings (which interventions each arm receives)
    - Intervention definitions (drugs, devices, procedures, etc.)
    - Study-to-intervention mappings (linking studies to their interventions)

    Interventions may have alternate names (e.g., brand vs generic drug names).
    These are extracted separately but inherit type and description from their
    parent intervention. The armGroupLabels field on interventions is excluded
    as a data source; arm-intervention relationships are derived solely from
    armGroups[].interventionNames to maintain a single source of truth.

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested arm and
            intervention data

    Returns:
        Six-element tuple containing:
            - arm_groups: Arm group dimension records
            - arm_interventions: Bridge table linking arms to intervention names
            - interventions: Primary intervention dimension records
            - study_interventions: Bridge table for primary interventions
            - intervention_aliases: Alternate name intervention records
            - study_intervention_aliases: Bridge table for alternate names


        All lists return empty if no arms/interventions exist for the study.
    """

    arm_groups = []
    arm_interventions = []

    interventions = []
    study_interventions = []
    intervention_aliases = []
    study_intervention_aliases = []

    arms_interventions_index = NON_SCALAR_FIELDS["arms_interventions"]["index_field"]
    arm_groups_list = study_data.get(f"{arms_interventions_index}.armGroups")

    if isinstance(arm_groups_list, (list, np.ndarray)) and len(arm_groups_list) > 0:
        for arm_group in arm_groups_list:
            arm_label = arm_group.get("label")
            arm_description = arm_group.get("description")
            arm_type = arm_group.get("type")

            arm_group_key = generate_key(
                study_key, arm_label, arm_description, arm_type
            )

            arm_groups.append(
                {
                    "study_key": study_key,
                    "arm_group_key": arm_group_key,
                    "arm_label": arm_label,
                    "arm_description": arm_description,
                    "arm_type": arm_type,
                }
            )

            arm_interventions_list = arm_group.get("interventionNames")
            if (
                isinstance(arm_interventions_list, (list, np.ndarray))
                and len(arm_interventions_list) > 0
            ):

                for arm_intervention in arm_interventions_list:
                    arm_intervention_name = standardize_intervention_name(
                        arm_intervention, ARMS_SOURCE
                    )
                    arm_intervention_key = generate_key(arm_intervention_name)

                    arm_interventions.append(
                        {
                            "study_key": study_key,
                            "arm_group_key": arm_group_key,
                            "arm_intervention_key": arm_intervention_key,
                            "arm_intervention_name": arm_intervention_name,
                        }
                    )

    interventions_list = study_data.get(f"{arms_interventions_index}.interventions")
    if (
        isinstance(interventions_list, (list, np.ndarray))
        and len(interventions_list) > 0
    ):
        for intervention in interventions_list:
            main_name = standardize_intervention_name(intervention.get("name"))
            intervention_type = intervention.get("type")

            intervention_key = generate_key(
                main_name
            )  # Only name is used to enable matching on both arms and interventions

            interventions.append(
                {
                    "intervention_key": intervention_key,
                    "intervention_name": main_name,
                    "intervention_type": intervention_type,
                }
            )

            study_interventions.append(
                {
                    "study_key": study_key,
                    "intervention_key": intervention_key,
                    "description": intervention.get(
                        "description"
                    ),  # study specific description
                }
            )

            other_names = intervention.get("otherNames")

            if isinstance(other_names, (list, np.ndarray)) and len(other_names) > 0:
                for other_name in other_names:
                    other_name = standardize_intervention_name(other_name)
                    if other_name == main_name:
                        continue  # some studies put the main name in the list of other names

                    intervention_alias_key = generate_key(other_name)
                    intervention_aliases.append(
                        {
                            "intervention_alias_key": intervention_alias_key,
                            "intervention_key": intervention_key,
                            # has its own key as other here could be main and vice versa in other studies.
                            # and it remains independent in the warehouse
                            "intervention_name": other_name,
                            "intervention_type": intervention_type,  # inherit from parent
                        }
                    )

                    study_intervention_aliases.append(
                        {
                            "study_key": study_key,
                            "intervention_alias_key": intervention_alias_key,
                            "intervention_key": intervention_key,
                            "description": intervention.get("description"),
                        }
                    )
            # armGroupLabels is excluded to avoid bi-directional inconsistencies due to human errors from source.
            # check documentation/excluded_fields.md for details

    return (
        arm_groups,
        arm_interventions,
        interventions,
        study_interventions,
        intervention_aliases,
        study_intervention_aliases,
    )
