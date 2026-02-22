from typing import Dict, Tuple
import pandas as pd
from include.etl.transformation.config import SCALAR_FIELDS


def extract_age(raw_age: str) -> Tuple:
    # e.g format - 20 Years, 2 Months
    age = raw_age.strip().replace(",", "")
    split_age = age.split()

    if len(split_age) == 2:
        return int(split_age[0]), split_age[1]
    elif len(split_age) == 1:
        if split_age[0].isdigit():
            return int(split_age[0]), "Unknown"
        else:
            return None, split_age[0]

    return None, "Unknown"


def transform_scalar_fields(study_key: str, study_data: pd.Series) -> Dict:
    """
    Transform scalar (non-nested) fields into the main study fact record.

    This function collects fields that have a one-to-one relationship with the study
    directly into the central study record.

    Field mappings are defined in SCALAR_FIELDS config, which maps output
    column names to their source paths in the flattened study data.

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing all study fields.

    Returns:
        Single dictionary representing one row in the study fact table,
        with study_key and all scalar fields defined in SCALAR_FIELDS.
        Missing fields will have None values.
    """
    study_record = dict()

    study_record["study_key"] = study_key
    for entity_key in SCALAR_FIELDS:
        if entity_key == "min_age":
            study_record["min_age_value"], study_record["min_age_metric"] = extract_age(
                study_data[entity_key]
            )
        if entity_key == "max_age":
            study_record["max_age_value"], study_record["max_age_metric"] = extract_age(
                study_data[entity_key]
            )

        index_field = SCALAR_FIELDS.get(entity_key)

        study_record[entity_key] = study_data.get(index_field)

    return study_record
