import pandas as pd
import numpy as np
from tests.unit.transformation.fixtures import study_key

from include.etl.transformation.core_transformation.modules.conditions import (
    transform_conditions_module,
)


def test_numpy_array_instead_of_list(study_key):
    """Test umpy arrays are handled the same as lists."""

    study_data = pd.Series(
        {
            "protocolSection.conditionsModule.conditions": np.array(
                ["Condition 1", "Condition 2"]
            ),
        }
    )

    conditions, *_ = transform_conditions_module("NCT123", study_key, study_data)

    assert len(conditions) == 2


def test_empty_list_returns_empty_output(study_key):
    """Test empty lists in source data return empty outputs."""

    study_data = pd.Series(
        {
            "protocolSection.conditionsModule.conditions": [],
            "protocolSection.conditionsModule.keywords": [],
        }
    )

    conditions, study_conditions, keywords, study_keywords = (
        transform_conditions_module("NCT123", study_key, study_data)
    )

    assert conditions == []
    assert keywords == []
