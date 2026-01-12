from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_interventions_browse_module(
    study_key: str, study_data: pd.Series
) -> Tuple:
    """
    Transform MeSH (Medical Subject Headings) intervention classifications from a study.

    interventions to MeSH terms, enabling standardized searching across studies
    for drugs, devices, procedures, and other intervention types.

    This module extracts four levels of the MeSH hierarchy:
    - Meshes: Direct MeSH term assignments for the study's interventions
    - Ancestors: Parent terms in the MeSH tree (broader categories)
    - Browse leaves: Terminal nodes in the browse hierarchy with relevance scores
    - Browse branches: Top-level category groupings

    Some MeSH terms contain comma-separated values which are split into
    individual records to normalize the data.

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested MeSH classification data


    Returns:
        Eight-element tuple containing dimension and bridge table pairs:
            - interventions_mesh, study_interventions_mesh: Direct MeSH assignments
            - interventions_mesh_ancestors, study_interventions_mesh_ancestors
            - interventions_browse_leaves, study_interventions_browse_leaves
            - interventions_browse_branches, study_interventions_browse_branches:
                Branch nodes with name, as_found, and relevance fields

        All lists return empty if no MeSH data exists for the study.

    """

    interventions_mesh = []
    study_interventions_mesh = []

    interventions_mesh_ancestors = []
    study_interventions_mesh_ancestors = []

    interventions_browse_leaves = []
    study_interventions_browse_leaves = []

    interventions_browse_branches = []
    study_interventions_browse_branches = []

    interventions_browse_index = NON_SCALAR_FIELDS["interventions_browse"][
        "index_field"
    ]

    meshes = study_data.get(f"{interventions_browse_index}.meshes")
    if isinstance(meshes, (list, np.ndarray)) and len(meshes) > 0:
        for mesh in meshes:
            mesh_terms = mesh.get("term")

            if isinstance(mesh_terms, str) and mesh_terms:
                terms = mesh_terms.split(",")
                for term in terms:
                    term = term.strip()
                    mesh_key = generate_key(term)
                    interventions_mesh.append(
                        {
                            "mesh_key": mesh_key,
                            "mesh_id": mesh.get("id"),
                            "mesh_term": term.lower(),
                        }
                    )

                    study_interventions_mesh.append(
                        {"mesh_key": mesh_key, "study_key": study_key}
                    )

    mesh_ancestors_list = study_data.get(f"{interventions_browse_index}.ancestors")
    if (
        isinstance(mesh_ancestors_list, (list, np.ndarray))
        and len(mesh_ancestors_list) > 0
    ):

        for mesh_ancestor in mesh_ancestors_list:
            ancestor_terms = mesh_ancestor.get("term")

            if isinstance(ancestor_terms, str) and ancestor_terms:
                terms = ancestor_terms.split(",")
                for term in terms:
                    term = term.strip()
                    ancestor_key = generate_key(term)
                    interventions_mesh_ancestors.append(
                        {
                            "mesh_ancestor_key": ancestor_key,
                            "study_key": study_key,
                            "mesh_ancestor_id": mesh_ancestor.get("id"),
                            "mesh_ancestor_term": term.lower(),
                        }
                    )

                    study_interventions_mesh_ancestors.append(
                        {"mesh_ancestor_key": ancestor_key, "study_key": study_key}
                    )

    mesh_browse_leaves = study_data.get(f"{interventions_browse_index}.browseLeaves")
    if (
        isinstance(mesh_browse_leaves, (list, np.ndarray))
        and len(mesh_browse_leaves) > 0
    ):
        for browse_leaf in mesh_browse_leaves:
            leaf_id = browse_leaf.get("id")
            leaf_key = generate_key(leaf_id)

            interventions_browse_leaves.append(
                {
                    "leaf_key": leaf_key,
                    "name": browse_leaf.get("name").lower(),
                    "as_found": browse_leaf.get("asFound"),
                    "relevance": browse_leaf.get("relevance"),
                }
            )

            study_interventions_browse_leaves.append(
                {"mesh_browse_leaf_key": leaf_key, "study_key": study_key}
            )

    mesh_browse_branches = study_data.get(
        f"{interventions_browse_index}.browseBranches"
    )
    if (
        isinstance(mesh_browse_branches, (list, np.ndarray))
        and len(mesh_browse_branches) > 0
    ):
        for browse_branch in mesh_browse_branches:
            branch_id = browse_branch.get("id")
            branch_key = generate_key(branch_id)

            interventions_browse_branches.append(
                {
                    "branch_key": branch_key,
                    "name": browse_branch.get("name").lower(),
                    "as_found": browse_branch.get("asFound"),
                    "relevance": browse_branch.get("relevance"),
                }
            )

            study_interventions_browse_branches.append(
                {"mesh_browse_branch_key": branch_key, "study_key": study_key}
            )

    # else:
    #     log.warning(f"No interventions_mesh found for study {study_key}, page - NCT ID {nct_id}")

    return (
        interventions_mesh,
        study_interventions_mesh,
        interventions_mesh_ancestors,
        study_interventions_mesh_ancestors,
        interventions_browse_leaves,
        study_interventions_browse_leaves,
        interventions_browse_branches,
        study_interventions_browse_branches,
    )
