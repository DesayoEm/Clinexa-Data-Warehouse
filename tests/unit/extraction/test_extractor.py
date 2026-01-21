import pytest
import json
import time
from unittest.mock import MagicMock, patch


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_init(mock_config, mock_state_handler, mock_context, mock_s3_hook):
    """Extractor initializes with correct default values."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    assert extractor.execution_date == "2026-01-15"
    assert extractor.max_retries == 3
    assert extractor.timeout == 30
    assert extractor.last_saved_page == 0


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_init_with_checkpoint(
    mock_config, mock_state_handler, mock_context, mock_s3_hook
):
    """Extractor initializes from checkpoint on retry."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 10,
        "next_page_url": "https://api.ctgov.com/studies?pageToken=token_11",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    assert extractor.last_saved_page == 10
    assert "token_11" in extractor.next_page_url


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_wait_if_needed_no_wait(
    mock_config, mock_state_handler, mock_context, mock_s3_hook
):
    """No wait when under rate limit."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }  #
    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    extractor.requests = []

    start = time.time()
    extractor.wait_if_needed()
    elapsed = time.time() - start

    assert elapsed < 1
    assert len(extractor.requests) == 1


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_wait_if_needed_prunes_old_requests(
    mock_config, mock_state_handler, mock_context, mock_s3_hook
):
    """Old request timestamps are pruned from the sliding window."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    # old timestamps
    old_time = time.time() - 120
    extractor.requests = [old_time, old_time + 1, old_time + 2]

    extractor.wait_if_needed()

    # only new one remains
    assert len(extractor.requests) == 1
    assert extractor.requests[0] > time.time() - 1


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_save_response(mock_config, mock_state_handler, mock_context, mock_s3_hook):
    """Response is saved to S3 as Parquet."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_config.CTGOV_DEST = "CTGOV"
    mock_config.RAW_DEST = "raw"

    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    data = {
        "studies": [
            {"nctId": "NCT00000001"},
            {"nctId": "NCT00000002"},
        ]
    }

    extractor.save_response(1, data)

    # Ensure S3 upload was called
    mock_s3_hook.load_bytes.assert_called_once()
    call_kwargs = mock_s3_hook.load_bytes.call_args[1]

    assert call_kwargs["bucket_name"] == "clinical-trials-bucket"
    assert call_kwargs["key"] == "CTGOV/raw/2026-01-15/1.parquet"
    assert call_kwargs["replace"] is True

    # ensure page counter incremented
    assert extractor.last_saved_page == 1


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_make_requests_success(
    mock_config,
    mock_state_handler,
    mock_requests,
    mock_context,
    mock_s3_hook,
    sample_api_response_last_page,
):
    """Successful extraction returns metadata."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    # mock last page with no nextPageToken
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_api_response_last_page
    mock_requests.get.return_value = mock_response

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    metadata = extractor.make_requests()

    assert metadata["pages_extracted"] == 1
    assert "data_location" in metadata


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_make_requests_retry_on_failure(
    mock_config, mock_state_handler, mock_requests, mock_context, mock_s3_hook
):
    """Request retries on non-200 response."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    fail_response = MagicMock()
    fail_response.status_code = 500

    success_response = MagicMock()
    success_response.status_code = 200
    success_response.json.return_value = {"studies": [{"nctId": "NCT001"}]}

    # First 2 calls fail, 3rd succeeds with last page
    mock_requests.get.side_effect = [fail_response, fail_response, success_response]

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    metadata = extractor.make_requests()

    #  3 attempts
    assert mock_requests.get.call_count == 3
    assert metadata["pages_extracted"] == 1


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_make_requests_exhaustion_error(
    mock_config, mock_state_handler, mock_requests, mock_context, mock_s3_hook
):
    """Raises RequestExhaustionError after max retries."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    # all calls fail
    fail_response = MagicMock()
    fail_response.status_code = 500
    mock_requests.get.return_value = fail_response

    from include.etl.extraction.extraction import Extractor
    from include.monitoring.exceptions import RequestExhaustionError

    extractor = Extractor(mock_context, mock_s3_hook, max_retries=3)

    with pytest.raises(RequestExhaustionError):
        extractor.make_requests()

    # Check checkpoint was saved before raising
    mock_state_handler.return_value.mark_checkpoint.assert_called()


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_make_requests_saves_manifest(
    mock_config,
    mock_state_handler,
    mock_requests,
    mock_context,
    mock_s3_hook,
    sample_api_response_last_page,
):
    """Manifest is saved to S3 on successful completion."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_api_response_last_page
    mock_requests.get.return_value = mock_response

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    extractor.make_requests()

    # check manifest was saved
    mock_s3_hook.load_string.assert_called_once()
    call_kwargs = mock_s3_hook.load_string.call_args[1]

    assert call_kwargs["bucket_name"] == "clinical-trials-bucket"
    assert "_manifest.json" in call_kwargs["key"]

    # check manifest content
    manifest = json.loads(call_kwargs["string_data"])
    assert "location" in manifest
    assert "metrics" in manifest
    assert "lineage" in manifest


@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_save_response_increments_page_counter(
    mock_config, mock_state_handler, mock_context, mock_s3_hook
):
    """Page counter increments after each successful save."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)

    assert extractor.last_saved_page == 0

    extractor.save_response(1, {"studies": []})
    assert extractor.last_saved_page == 1

    extractor.save_response(2, {"studies": []})
    assert extractor.last_saved_page == 2


@patch("include.etl.extraction.extraction.requests")
@patch("include.etl.extraction.extraction.StateHandler")
@patch("include.etl.extraction.extraction.config")
def test_token_tracking(
    mock_config, mock_state_handler, mock_requests, mock_context, mock_s3_hook
):
    """Previous and current tokens are tracked correctly."""
    mock_config.FIRST_PAGE_URL = "https://api.ctgov.com/studies"
    mock_config.BASE_URL = "https://api.ctgov.com/studies?pageToken="
    mock_config.CLINEXA_BUCKET = "clinical-trials-bucket"
    mock_state_handler.return_value.determine_state.return_value = {
        "last_saved_page": 0,
        "next_page_url": "https://api.ctgov.com/studies",
    }

    # Pg 1 returns token for pg 2, pg 2 has no token
    response_page1 = MagicMock()
    response_page1.status_code = 200
    response_page1.json.return_value = {
        "studies": [{"nctId": "NCT001"}],
        "nextPageToken": "token_page_2",
    }

    response_page2 = MagicMock()
    response_page2.status_code = 200
    response_page2.json.return_value = {"studies": [{"nctId": "NCT002"}]}

    mock_requests.get.side_effect = [response_page1, response_page2]

    from include.etl.extraction.extraction import Extractor

    extractor = Extractor(mock_context, mock_s3_hook)
    metadata = extractor.make_requests()

    # final state must have previous_token from page 1
    assert metadata["last_valid_token"] == "token_page_2"
    assert metadata["final_token"] is None  # Last page has no token
