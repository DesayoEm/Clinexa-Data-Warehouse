import pandas as pd
from include.etl.transformation.core_transformation.modules.arms_intervention import (
    transform_arms_interventions_module,
    standardize_intervention_name,
    ARMS_SOURCE,
)
from tests.unit.transformation.fixtures import (
    study_key,
    full_study_data,
    empty_study_data,
)
from include.etl.transformation.utils import generate_key


def test_empty_data_returns_empty_tuples(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    result = transform_arms_interventions_module(study_key, empty_study_data)
    assert all(lst == [] for lst in result)


def test_arm_groups_extracted(study_key, full_study_data):
    """Test arm groups are properly extracted."""
    arm_groups, arm_interventions, *_ = transform_arms_interventions_module(
        study_key, full_study_data
    )

    assert len(arm_groups) == 2
    assert arm_groups[0]["arm_label"] == "Treatment Arm"
    assert arm_groups[0]["arm_type"] == "EXPERIMENTAL"


def test_arm_interventions_standardized(study_key, full_study_data):
    """Test arm intervention names are standardized."""
    arm_groups, arm_interventions, *_ = transform_arms_interventions_module(
        study_key, full_study_data
    )

    assert len(arm_interventions) == 3
    intervention_names = [ai["arm_intervention_name"] for ai in arm_interventions]
    assert "Metformin" in intervention_names
    assert "Lisinopril" in intervention_names
    assert "Placebo" in intervention_names


def test_interventions_extracted(study_key, full_study_data):
    """Test main interventions are properly extracted."""
    _, _, interventions, study_interventions, *_ = transform_arms_interventions_module(
        study_key, full_study_data
    )

    assert len(interventions) == 3
    assert interventions[0]["intervention_name"] == "Metformin"
    assert interventions[0]["intervention_type"] == "DRUG"


def test_other_names_extracted(study_key, full_study_data):
    """Test other intervention names (aliases) are extracted."""
    *_, other_names, study_aliases = transform_arms_interventions_module(
        study_key, full_study_data
    )

    assert len(other_names) == 2
    other_name_values = [n["intervention_name"] for n in other_names]
    assert "Glucophage" in other_name_values
    assert "Fortamet" in other_name_values


def test_duplicate_other_name_skipped(study_key):
    """Test other names that match main name are skipped."""

    study_data = pd.Series(
        {
            "protocolSection.armsInterventionsModule.interventions": [
                {
                    "name": "Aspirin",
                    "type": "DRUG",
                    "otherNames": ["Aspirin", "ASA"],
                },
            ],
        }
    )

    *_, other_names, _ = transform_arms_interventions_module(study_key, study_data)

    assert len(other_names) == 1
    assert other_names[0]["intervention_name"] == "ASA"


def test_arm_intervention_key_matches_intervention_key(study_key):
    """Arm intervention keys should match intervention keys for joining."""

    study_data = pd.Series(
        {
            "protocolSection.armsInterventionsModule.armGroups": [
                {
                    "label": "Arm 1",
                    "type": "EXPERIMENTAL",
                    "description": "Desc",
                    "interventionNames": ["Drug: Metformin"],
                },
            ],
            "protocolSection.armsInterventionsModule.interventions": [
                {
                    "name": "Metformin",
                    "type": "DRUG",
                    "description": "Desc",
                },
            ],
        }
    )

    _, arm_interventions, interventions, *_ = transform_arms_interventions_module(
        study_key, study_data
    )
    assert (
        arm_interventions[0]["arm_intervention_key"]
        == interventions[0]["intervention_key"]
    )


def test_none_input_returns_none():
    """Test None input returns None."""
    assert standardize_intervention_name(None) is None
    assert standardize_intervention_name("") is None


def test_strips_arm_prefix():
    """Test ARMS_SOURCE strips 'Type: Name' prefix."""
    result = standardize_intervention_name("Drug: Metformin", ARMS_SOURCE)
    assert result == "Metformin"


def test_alias_substitution():
    """Test known aliases are substituted."""
    assert standardize_intervention_name("sham") == "Placebo"
    assert standardize_intervention_name("5-FU") == "Fluorouracil"
    assert standardize_intervention_name("T-DM1") == "Trastuzumab"


def test_placebo_normalization():
    """Test placebo variants are normalized."""
    assert standardize_intervention_name("Placebo matched to Drug X") == "Placebo"
    assert standardize_intervention_name("placebo for comparison") == "Placebo"


def test_capitalization():
    """Test interventions are capitalized."""
    assert (
        standardize_intervention_name("metformin hydrochloride")
        == "Metformin Hydrochloride"
    )


def test_preserves_uppercase_abbreviations():
    """Test all-caps interventions are preserved."""
    assert standardize_intervention_name("ACE inhibitor") == "ACE Inhibitor"
