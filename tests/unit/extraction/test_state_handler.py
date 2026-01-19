import json
from unittest.mock import MagicMock, patch


@patch("include.etl.extraction.checkpoint.config")
def test_init(mock_config, mock_context):
    """StateHandler initializes with context and execution date."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context)

    assert handler.execution_date == "2026-01-15"
    assert handler.context == mock_context


@patch("include.etl.extraction.checkpoint.config")
def test_determine_state_first_run(mock_config, mock_context):
    """First run (try_number=1) returns default state."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context)
    state = handler.determine_state()

    assert state["last_saved_page"] == 0
    assert state["last_saved_token"] is None
    assert state["next_page_url"] == "https://api.ctgov.com/studies"


@patch("include.etl.extraction.checkpoint.Variable")
@patch("include.etl.extraction.checkpoint.config")
def test_determine_state_retry_with_checkpoint(
    mock_config, mock_variable, mock_context_retry
):
    """Retry run loads checkpoint from Airflow Variable."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="

    checkpoint_data = {
        "last_saved_page": 5,
        "last_saved_token": "token_page_6",
    }
    mock_variable.get.return_value = json.dumps(checkpoint_data)

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context_retry)
    state = handler.determine_state()

    assert state["last_saved_page"] == 5
    assert state["last_saved_token"] == "token_page_6"
    assert (
        state["next_page_url"] == "https://api.ctgov.com/studies?pageToken=token_page_6"
    )


@patch("include.etl.extraction.checkpoint.Variable")
@patch("include.etl.extraction.checkpoint.config")
def test_determine_state_retry_no_checkpoint(
    mock_config, mock_variable, mock_context_retry
):
    """Retry run without checkpoint falls back to default state."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_variable.get.side_effect = KeyError("No checkpoint found")

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context_retry)
    state = handler.determine_state()

    assert state["last_saved_page"] == 0
    assert state["last_saved_token"] is None


@patch("include.etl.extraction.checkpoint.Variable")
@patch("include.etl.extraction.checkpoint.config")
def test_determine_state_invalid_json(mock_config, mock_variable, mock_context_retry):
    """Invalid JSON in checkpoint falls back to default state."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_variable.get.return_value = "not valid json{{"

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context_retry)
    state = handler.determine_state()

    assert state["last_saved_page"] == 0


@patch("include.etl.extraction.checkpoint.Variable")
@patch("include.etl.extraction.checkpoint.config")
def test_save_checkpoint(mock_config, mock_variable, mock_context):
    """Checkpoint is saved to Airflow Variable with correct key and value."""
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(mock_context)
    handler.save_checkpoint(
        previous_token="token_page_4",
        last_saved_page=5,
        last_saved_token="token_page_6",
    )

    # check Variable.set was called with correct args
    mock_variable.set.assert_called_once()
    call_args = mock_variable.set.call_args

    assert call_args[0][0] == "extract_studies_2026-01-15"

    saved_data = json.loads(call_args[0][1])
    assert saved_data["last_saved_page"] == 5
    assert saved_data["last_saved_token"] == "token_page_6"
    assert saved_data["previous_token"] == "token_page_4"


@patch("include.etl.extraction.checkpoint.config")
def test_determine_state_no_task_instance(mock_config):
    """Missing task instance returns default state."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"

    context = MagicMock()
    context.get.return_value = None  # No ti

    from include.etl.extraction.checkpoint import StateHandler

    handler = StateHandler(context)
    state = handler.determine_state()

    assert state["last_saved_page"] == 0
