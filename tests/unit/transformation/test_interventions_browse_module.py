from include.etl.transformation.core_transformation.modules.interventions_browse import (
    transform_interventions_browse_module,
)
from tests.unit.transformation.fixtures import (
    study_key,
    browse_study_data,
    empty_study_data,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    result = transform_interventions_browse_module(study_key, empty_study_data)
    assert all(lst == [] for lst in result)


def test_mesh_terms_extracted(study_key, browse_study_data):
    """Test intervention MeSH terms are extracted as intended."""
    mesh, study_mesh, *_ = transform_interventions_browse_module(
        study_key, browse_study_data
    )

    assert len(mesh) == 1
    assert mesh[0]["mesh_term"] == "metformin"


def test_ancestors_extracted(study_key, browse_study_data):
    """Test intervention ancestors are extracted."""
    _, _, ancestors, *_ = transform_interventions_browse_module(
        study_key, browse_study_data
    )

    assert len(ancestors) == 1
    assert ancestors[0]["ancestor_term"] == "hypoglycemic agents"


def test_browse_leaves_extracted(study_key, browse_study_data):
    """Test intervention browse leaves are extracted."""
    *_, leaves, _, _, _ = transform_interventions_browse_module(
        study_key, browse_study_data
    )

    assert len(leaves) == 1
    assert leaves[0]["name"] == "metformin"
