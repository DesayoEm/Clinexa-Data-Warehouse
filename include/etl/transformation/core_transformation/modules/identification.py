from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_identification_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Transform secondary identifiers and NCT ID aliases from a clinical trial.

    Clinical trials often have multiple identifiers beyond the primary NCT ID:
    - Secondary IDs: Identifiers from other registries, sponsors, or bodies


    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested identification
            data at the path specified in NON_SCALAR_FIELDS["identification"].

    Returns:
        Two-element tuple containing:
            - secondary_ids: Secondary identifier records
            - nct_aliases: NCT alias records

        All lists return empty if no secondary IDs/aliases exist for the study.
    """

    secondary_ids = []
    nct_aliases = []

    identification_index = NON_SCALAR_FIELDS["identification"]["index_field"]

    # Secondary id infos
    secondary_id_infos = study_data.get(f"{identification_index}.secondaryIdInfos")

    if (
        isinstance(secondary_id_infos, (list, np.ndarray))
        and len(secondary_id_infos) > 0
    ):
        for secondary_id_info in secondary_id_infos:
            secondary_id = secondary_id_info.get("id")
            secondary_id_key = generate_key(study_key, secondary_id)

            secondary_ids.append(
                {
                    "secondary_id_key": secondary_id_key,
                    "study_key": study_key,
                    "id": secondary_id,
                    "type": secondary_id_info.get("type"),
                    "domain": secondary_id_info.get("domain"),
                    "link": secondary_id_info.get("link"),
                }
            )

    # nct id aliases
    nct_id_aliases = study_data.get(f"{identification_index}.nctIdAliases")

    if isinstance(nct_id_aliases, (list, np.ndarray)) and len(nct_id_aliases) > 0:
        for nct_id_alias in nct_id_aliases:
            id_alias_key = generate_key(nct_id_alias)

            nct_aliases.append(
                {
                    "study_key": study_key,
                    "id_alias_key": id_alias_key,
                    "id_alias": nct_id_alias,
                }
            )

    return secondary_ids, nct_aliases
