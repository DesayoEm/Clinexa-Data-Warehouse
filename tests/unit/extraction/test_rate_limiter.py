import time
from unittest.mock import patch


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_rate_limit_window_calculation(
    mock_config, mock_state_handler, mock_context, mock_s3_hook
):
    """Rate limiter correctly tracks requests within window."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    extractor.max_requests = 5
    extractor.window = 60

    # 4 requests within window
    now = time.time()
    extractor.requests = [now - 10, now - 8, now - 5, now - 2]

    extractor.wait_if_needed()

    # must have 5 requests
    assert len(extractor.requests) == 5


@patch("include.etl.extraction.extraction.time.sleep")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_rate_limit_triggers_sleep(
    mock_config, mock_state_handler, mock_sleep, mock_context, mock_s3_hook
):
    """Rate limiter sleeps when at max capacity."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    extractor.max_requests = 3
    extractor.window = 60

    # filling up the request window
    now = time.time()
    extractor.requests = [now - 30, now - 20, now - 10]

    extractor.wait_if_needed()

    # must trigger sleep
    mock_sleep.assert_called_once()
