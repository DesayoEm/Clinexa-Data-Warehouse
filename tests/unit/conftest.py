import sys
import pytest
from unittest.mock import MagicMock

from tests.unit.transformation.fixtures import (
    study_key,
    full_study_data,
    minimal_study_data,
    browse_study_data,
    results_study_data,
    results_study_data,
    empty_study_data,
    nct_id,
)


from tests.unit.extraction.fixtures import (
    mock_context,
    mock_context_retry,
    mock_s3_hook,
    sample_api_response,
    sample_api_response_last_page,
)

import sys
from unittest.mock import MagicMock

# mock config
mock_config = MagicMock()
mock_config.BASE_URL = "https://mock.url"
mock_config.FIRST_PAGE_URL = "https://mock.url"
mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
mock_config.CTGOV_DEST = "CTGOV"
mock_config.RAW_DEST = "raw"
mock_config.AWS_ACCESS_KEY_ID = "XXXXXXXXXXXXX"
mock_config.AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXX"
mock_config.AWS_REGION = "mock-west-1"
mock_config.DB_NAME = "mock-db-name"
mock_config.DB_USER = "mock-user"
mock_config.DB_PASSWORD = "pwd-pwd"
mock_config.DB_CONN_STR = "mock-str"


# mock modules for airflow components
mock_airflow = MagicMock()
mock_airflow.utils = MagicMock()
mock_airflow.utils.context = MagicMock()
mock_airflow.utils.context.Context = MagicMock
mock_airflow.models = MagicMock()
mock_airflow.models.Variable = MagicMock()
mock_airflow.providers = MagicMock()
mock_airflow.providers.amazon = MagicMock()
mock_airflow.providers.amazon.aws = MagicMock()
mock_airflow.providers.amazon.aws.hooks = MagicMock()
mock_airflow.providers.amazon.aws.hooks.s3 = MagicMock()


AIRFLOW_MODULES = {
    "airflow": mock_airflow,
    "airflow.utils": mock_airflow.utils,
    "airflow.utils.context": mock_airflow.utils.context,
    "airflow.models": mock_airflow.models,
    "airflow.providers": mock_airflow.providers,
    "airflow.providers.amazon": mock_airflow.providers.amazon,
    "airflow.providers.amazon.aws": mock_airflow.providers.amazon.aws,
    "airflow.providers.amazon.aws.hooks": mock_airflow.providers.amazon.aws.hooks,
    "airflow.providers.amazon.aws.hooks.s3": mock_airflow.providers.amazon.aws.hooks.s3,
}

for module_name, mock_module in AIRFLOW_MODULES.items():
    sys.modules[module_name] = mock_module
sys.modules["airflow.models"].Variable = MagicMock()


sys.modules["config.env_config"] = MagicMock()
sys.modules["config.env_config"].config = mock_config


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires Airflow)"
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """Skip integration tests by default unless explicitly requested."""
    run_integration = config.getoption("--run-integration", default=False)
    if not run_integration:
        skip_integration = pytest.mark.skip(
            reason="need --run-integration option to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests",
    )
