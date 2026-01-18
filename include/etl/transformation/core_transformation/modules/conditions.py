from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_conditions_module(
    nct_id: str, study_key: str, study_data: pd.Series
) -> Tuple:
    """
    Extract medical conditions and keywords from a clinical trial study.

    Conditions are the diseases, disorders, or health issues being studied
    (e.g., "Type 2 Diabetes", "Breast Cancer"). Keywords are sponsor-provided
    search terms that may include conditions, interventions, or other relevant
    descriptors to aid discoverability.

    Both are modeled as dimension tables with bridge tables to support the
    many-to-many relationship between studies and conditions/keywords.

    Args:
        nct_id: ClinicalTrials.gov identifier (e.g., "NCT12345678").
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested condition and
            keyword data at the path specified in
            NON_SCALAR_FIELDS["conditions"].

    Returns:
        Four-element tuple containing:
            - conditions: Condition dimension records
            - study_conditions: Bridge table linking studies to conditions
            - keywords: Keyword dimension records
            - study_keywords: Bridge table linking studies to keywords

        All lists return empty if no conditions/keywords exist for the study.
    """
    conditions = []
    study_conditions = []
    keywords = []
    study_keywords = []

    conditions_index = NON_SCALAR_FIELDS["conditions"]["index_field"]

    conditions_list = study_data.get(f"{conditions_index}.conditions")

    if isinstance(conditions_list, (list, np.ndarray)) and len(conditions_list) > 0:
        for condition in conditions_list:
            condition_key = generate_key(condition)

            conditions.append(
                {"condition_key": condition_key, "condition_name": condition.lower()}
            )

            study_conditions.append(
                {
                    "study_key": study_key,
                    "condition_key": condition_key,
                }
            )

    keywords_list = study_data.get(f"{conditions_index}.keywords")

    if isinstance(keywords_list, (list, np.ndarray)) and len(keywords_list) > 0:

        for keyword in keywords_list:
            keyword_key = generate_key(keyword)

            keywords.append(
                {"keyword_key": keyword_key, "keyword_name": keyword.lower()}
            )

            study_keywords.append(
                {
                    "study_key": study_key,
                    "keyword_key": keyword_key,
                }
            )

    return conditions, study_conditions, keywords, study_keywords
