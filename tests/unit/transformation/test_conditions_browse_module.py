
import pandas as pd
from include.etl.transformation.core_transformation.modules.conditions_browse import transform_conditions_browse_module
from tests.unit.transformation.fixtures import study_key, browse_study_data, empty_study_data


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """ Test empty study data returns empty lists."""
    result = transform_conditions_browse_module(study_key, empty_study_data)
    assert all(lst == [] for lst in result)

def test_mesh_terms_extracted_lowercase(study_key, browse_study_data):
    """Test MeSH terms are extracted and lowercased, comma-separated values are split."""

    mesh, study_mesh, *_ = transform_conditions_browse_module(study_key, browse_study_data)

    # "Diabetes Mellitus, Type 2" is split into 2 terms, plus "Hypertension" = 3 total
    assert len(mesh) == 3
    terms = [m["mesh_term"] for m in mesh]
    assert "diabetes mellitus" in terms
    assert "type 2" in terms
    assert "hypertension" in terms

def test_ancestors_extracted(study_key, browse_study_data):
    """Test ancestor terms are properly extracted."""
    _, _, ancestors, study_ancestors, *_ = transform_conditions_browse_module(study_key, browse_study_data)

    assert len(ancestors) == 1
    assert ancestors[0]["term"] == "metabolic diseases"

def test_browse_leaves_extracted(study_key, browse_study_data):
    """Test browse leaves are extracted."""

    *_, leaves, study_leaves, _, _ = transform_conditions_browse_module(study_key, browse_study_data)

    assert len(leaves) == 1
    assert leaves[0]["name"] == "diabetes mellitus"
    assert leaves[0]["relevance"] == "HIGH"

def test_browse_branches_extracted(study_key, browse_study_data):
    """Test branches are properly extracted."""
    *_, branches, study_branches = transform_conditions_browse_module(study_key, browse_study_data)

    assert len(branches) == 1
    assert branches[0]["name"] == "nutritional and metabolic diseases"

def test_comma_separated_mesh_terms_split(study_key):
    """Comma-separated MeSH terms are split into individual records."""

    study_data = pd.Series({
        "derivedSection.conditionBrowseModule.meshes": [
            {"id": "D000001", "term": "Term One, Term Two, Term Three"},
        ],
    })

    mesh, *_ = transform_conditions_browse_module(study_key, study_data)

    assert len(mesh) == 3
    terms = [m["mesh_term"] for m in mesh]
    assert "term one" in terms
    assert "term two" in terms
    assert "term three" in terms