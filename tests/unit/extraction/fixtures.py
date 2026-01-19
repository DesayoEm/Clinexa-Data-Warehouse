import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    context = MagicMock()
    context.get.side_effect = lambda key: {
        "ds": "2026-01-15",
        "task_instance": MagicMock(task_id="extract_studies", try_number=1),
        "dag": MagicMock(dag_id="clinical_trials_etl"),
        "run_id": "manual__2026-01-15T00:00:00+00:00",
    }.get(key)
    context.__getitem__ = lambda self, key: {
        "ds": "2026-01-15",
        "task_instance": MagicMock(task_id="extract_studies", try_number=1),
        "dag": MagicMock(dag_id="clinical_trials_etl"),
        "run_id": "manual__2026-01-15T00:00:00+00:00",
    }.get(key)
    return context


@pytest.fixture
def mock_context_retry():
    """Create a mock Airflow context for retry scenario (try_number > 1)."""
    ti = MagicMock(task_id="extract_studies", try_number=2)
    context = MagicMock()
    context.get.side_effect = lambda key: {
        "ds": "2026-01-15",
        "task_instance": ti,
        "dag": MagicMock(dag_id="clinical_trials_etl"),
        "run_id": "manual__2026-01-15T00:00:00+00:00",
    }.get(key)
    return context


@pytest.fixture
def mock_s3_hook():
    """Create a mock S3 hook."""
    hook = MagicMock()
    hook.load_bytes = MagicMock()
    hook.load_string = MagicMock()
    return hook


@pytest.fixture
def sample_api_response():
    """Sample API response with pagination token."""
    return {
        "studies": [
            {"protocolSection": {"identificationModule": {"nctId": "NCT00000001"}}},
            {"protocolSection": {"identificationModule": {"nctId": "NCT00000002"}}},
        ],
        "nextPageToken": "token_page_2",
    }


@pytest.fixture
def sample_api_response_last_page():
    """Sample API response for last page."""
    return {
        "studies": [
            {"protocolSection": {"identificationModule": {"nctId": "NCT00000099"}}},
        ],
        # nextPageToken is always absent on the last page
    }
