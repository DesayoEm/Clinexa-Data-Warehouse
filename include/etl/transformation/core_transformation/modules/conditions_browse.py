from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_conditions_browse_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Transform MeSH (Medical Subject Headings) condition classifications from a study.

    MeSH is the NLM's controlled vocabulary thesaurus used for indexing biomedical
    literature.

    This module extracts four levels of the MeSH hierarchy:
    - Meshes: Direct MeSH term assignments for the study's conditions
    - Ancestors: Parent terms in the MeSH tree (broader categories)
    - Browse leaves: Terminal nodes in the browse hierarchy with relevance scores
    - Browse branches: Top-level category groupings (e.g., "Nervous System Diseases")

    Some MeSH terms contain comma-separated values which are split into
    individual records to normalize the data.

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested MeSH classification
            data at the path specified in NON_SCALAR_FIELDS["conditions_browse"].

    Returns:
        Eight-element tuple containing dimension and bridge table pairs:
            - condition_meshes, study_condition_meshes.sql: Direct MeSH assignments
            - condition_mesh_ancestors, study_condition_mesh_ancestors
            - condition_browse_leaves, study_condition_browse_leaves
            - condition_browse_branches, study_condition_browse_branches

        All lists return empty if no MeSH data exists for the study.
    """

    condition_meshes = []
    study_condition_meshes = []

    condition_mesh_ancestors = []
    study_condition_mesh_ancestors = []

    condition_browse_leaves = []
    study_condition_browse_leaves = []

    condition_browse_branches = []
    study_condition_browse_branches = []

    conditions_browse_index = NON_SCALAR_FIELDS["conditions_browse"]["index_field"]

    meshes = study_data.get(f"{conditions_browse_index}.meshes")
    if isinstance(meshes, (list, np.ndarray)) and len(meshes) > 0:
        for mesh in meshes:
            mesh_terms = mesh.get("term")

            if isinstance(mesh_terms, str) and mesh_terms:
                terms = mesh_terms.split(",")
                for term in terms:
                    term = term.strip()
                    mesh_key = generate_key(term)
                    condition_meshes.append(
                        {
                            "mesh_key": mesh_key,
                            "mesh_id": mesh.get("id"),
                            "mesh_term": term.lower(),
                        }
                    )

                    study_condition_meshes.append(
                        {"mesh_key": mesh_key, "study_key": study_key}
                    )

    ancestors_list = study_data.get(f"{conditions_browse_index}.ancestors")
    if isinstance(ancestors_list, (list, np.ndarray)) and len(ancestors_list) > 0:
        for ancestor in ancestors_list:
            ancestor_terms = ancestor.get("term")

            if isinstance(ancestor_terms, str) and ancestor_terms:
                terms = ancestor_terms.split(
                    ","
                )  # sometimes MeSH terms are comma separated keywords
                for term in terms:
                    term = term.strip()
                    ancestor_key = generate_key(term)
                    condition_mesh_ancestors.append(
                        {
                            "ancestor_key": ancestor_key,
                            "ancestor_id": ancestor.get("id"),
                            "term": term.lower(),
                        }
                    )

                    study_condition_mesh_ancestors.append(
                        {"ancestor_key": ancestor_key, "study_key": study_key}
                    )

    mesh_browse_leaves = study_data.get(f"{conditions_browse_index}.browseLeaves")
    if (
        isinstance(mesh_browse_leaves, (list, np.ndarray))
        and len(mesh_browse_leaves) > 0
    ):
        for browse_leaf in mesh_browse_leaves:
            leaf_id = browse_leaf.get("id")
            leaf_key = generate_key(leaf_id)

            condition_browse_leaves.append(
                {
                    "leaf_key": leaf_key,
                    "leaf_id": leaf_id,
                    "name": browse_leaf.get("name").lower(),
                    "as_found": browse_leaf.get("asFound"),
                    "relevance": browse_leaf.get("relevance"),
                }
            )

            study_condition_browse_leaves.append(
                {"leaf_key": leaf_key, "study_key": study_key}
            )

    mesh_browse_branches = study_data.get(f"{conditions_browse_index}.browseBranches")

    if (
        isinstance(mesh_browse_branches, (list, np.ndarray))
        and len(mesh_browse_branches) > 0
    ):
        for browse_branch in mesh_browse_branches:
            branch_abbrev = browse_branch.get("abbrev")
            branch_name = browse_branch.get("name")
            branch_key = generate_key(branch_abbrev, branch_name)

            condition_browse_branches.append(
                {
                    "branch_key": branch_key,
                    "abbrev": branch_abbrev.lower(),
                    "name": branch_name.lower(),
                }
            )

            study_condition_browse_branches.append(
                {"branch_key": branch_key, "study_key": study_key}
            )

    return (
        condition_meshes,
        study_condition_meshes,
        condition_mesh_ancestors,
        study_condition_mesh_ancestors,
        condition_browse_leaves,
        study_condition_browse_leaves,
        condition_browse_branches,
        study_condition_browse_branches,
    )
