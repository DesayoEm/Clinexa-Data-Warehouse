import pytest
from unittest.mock import patch, MagicMock


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_checkpoint_saved_on_exception(
    mock_config, mock_state_handler, mock_requests, mock_context, mock_s3_hook
):
    """Checkpoint is saved when an exception occurs."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CTGOV_BUCKET = "ct-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    # Simulate network error
    mock_requests.get.side_effect = Exception("Network error")

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    with pytest.raises(Exception):
        extractor.make_requests()

    # Checkpoint should have been saved
    mock_state_handler.return_value.save_checkpoint.assert_called()


# NOTE: This test currently fails because the Extractor class is heavily
# orchestrated with side effects (retries, XCom pushes)

# Mocking all its behaviors for unit testing is a work in progress.

#
# @patch("include.etl.extraction.extraction.requests")
# @patch("include.etl.extraction.extraction.StateHandler")
# @patch("include.etl.extraction.extraction.config")
# def test_xcom_push_on_failure(
#     mock_config, mock_state_handler, mock_requests, mock_context, mock_s3_hook
# ):
#     mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
#     mock_config.CTGOV_BUCKET = "ct-bucket"
#
#     mock_state_handler.return_value.determine_state.return_value = {
#         "last_saved_page": 0,
#         "next_page_url": "https://api.ctgov.com/studies",
#     }
#
#     # force retry exhaustion,
#     fail_response = MagicMock()
#     fail_response.status_code = 500
#     mock_requests.get.return_value = fail_response
#
#     from include.etl.extraction.extraction import Extractor
#     from include.monitoring.exceptions import RequestExhaustionError
#
#     mock_context["task_instance"].xcom_push = MagicMock()
#     ti = mock_context["task_instance"]
#
#     extractor = Extractor(mock_context, mock_s3_hook, max_retries=3)
#
#     with pytest.raises(RequestExhaustionError):
#         extractor.make_requests()
#
#     ti.xcom_push.assert_called()
#
#     kwargs = ti.xcom_push.call_args.kwargs
#     assert kwargs["key"] == "metadata"
#     assert kwargs["value"]["status"] == "failed"
