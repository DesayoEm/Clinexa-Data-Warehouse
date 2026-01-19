from include.etl.transformation.core_transformation.modules.references import (
    transform_reference_module,
)


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""

    refs, links, ipds = transform_reference_module(study_key, empty_study_data)

    assert refs == []
    assert links == []
    assert ipds == []


def test_references_extracted(study_key, full_study_data):
    """Test literature references are properly extracted."""
    refs, links, ipds = transform_reference_module(study_key, full_study_data)

    assert len(refs) == 1
    assert refs[0]["pmid"] == "12345678"
    assert refs[0]["type"] == "RESULT"


def test_links_extracted(study_key, full_study_data):
    """Test external links are extracted."""
    refs, links, ipds = transform_reference_module(study_key, full_study_data)

    assert len(links) == 1
    assert links[0]["label"] == "Study Website"
    assert links[0]["url"] == "http://study.example.com"


def test_ipd_references_extracted(study_key, full_study_data):
    """Test IPD sharing references are extracted."""
    refs, links, ipds = transform_reference_module(study_key, full_study_data)

    assert len(ipds) == 1
    assert ipds[0]["type"] == "STUDY_PROTOCOL"
