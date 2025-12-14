import requests
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict
import json

from airflow.models import Variable
from airflow.utils.context import Context

from ct_gov.include.handlers.rate_limit_handler import RateLimiterHandler
from ct_gov.include.handlers.s3_handler import S3Handler
from ct_gov.include.tests.extractor_stress_test import FailureGenerator
from ct_gov.include.exceptions import RequestTimeoutError
from ct_gov.include.config import config
from ct_gov.include.notification_middleware.extractor_middleware import (
    persist_extraction_state_before_failure,
)

from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


class StateHandler:
    """Stateless utility class for loading extraction state from Airflow Variables."""

    def __init__(self, context: Context):
        self.context = context
        self.execution_date = self.context.get("ds")

    def determine_state(self) -> Dict:
        """
        Determines where to start extraction by checking for previous state in Variables.
        Returns a new ExtractorState instance with either:
        - Checkpoint data from a previous failed run (resume from where we left off)
        - Default values (start fresh from page 0)
        """

        log.info("Determining starting point for extractor...")
        default_state = {
            "last_saved_page": 0,
            "last_saved_token": None,
            "next_page_url": config.FIRST_PAGE_URL,
        }

        ti = self.context.get("task_instance")
        if not ti:
            log.warning("No task instance found in context, starting fresh")
            return default_state

        log.info(f"Current try_number: {ti.try_number}")
        if ti.try_number == 1:
            log.info("First run. Starting fresh extraction")
            return default_state

        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        try:
            checkpoint_json = Variable.get(checkpoint_key)
            checkpoint = json.loads(checkpoint_json)
            last_saved_page = checkpoint.get("last_saved_page", 0)

            log.info(
                f"Checkpoint loaded - Key: {checkpoint_key}, Last saved: {last_saved_page}"
            )
            log.info(f"  Resuming from page {last_saved_page + 1}")

        except KeyError:
            log.info(f"No checkpoint found for key: {checkpoint_key}")
            log.info(f"  Starting fresh from page 0")
            return default_state

        except json.JSONDecodeError as e:
            log.error(
                f"Failed to parse checkpoint JSON: {e}\n"
                f"JSON DATA\n\n"
                f"{checkpoint_json}"
            )

            log.info(f"Starting fresh from page 0")
            return default_state

        except Exception as e:
            log.info(
                f"ERROR finding checkpoint for key: {checkpoint_key} \n Error: {e}"
            )
            log.info(f"Defaulting to 0")
            return default_state

    def save_checkpoint(self, last_saved_page: int, last_saved_token: str) -> None:
        """
        Save checkpoint to Airflow Variable for retry recovery.
        Overwrites previous run for same task_id + execution_date.
        """
        ti = self.context.get("task_instance")
        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        next_page_url = f"{config.BASE_URL}{last_saved_token}"
        checkpoint_value = {
            "last_saved_page": last_saved_page,
            "last_saved_token": None,
            "next_page_url": next_page_url
        }

        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        log.info(
            f"Checkpoint saved - Key: {checkpoint_key},L ast saved: {last_saved_page}"
        )

    def clear_checkpoints(self):
        """Manually clear checkpoint"""
        checkpoint_key = f"{self.execution_date}"

        try:
            Variable.delete(checkpoint_key)
            log.info(f"Checkpoint cleared - Key: {checkpoint_key}")
        except KeyError:
            log.info(f"No checkpoint to clear for key: {checkpoint_key}")


class Extractor:
    def __init__(
        self, context: Context, s3_hook, timeout: int = 30, max_retries: int = 3
    ):

        self.context = context
        self.execution_date = self.context.get("ds")
        self.state = StateHandler(self.context)
        self.timeout: int = timeout
        self.max_retries: int = max_retries
        self.last_saved_page: int = 0
        self.next_page_url: str | None = None

        self.last_saved_page = self.state.determine_state().get("last_saved_page")
        self.next_page_url = self.state.determine_state().get("next_page_url")

        self.s3 = s3_hook
        self.failure_generator = FailureGenerator(True, 0.5)
        self.rate_limit_handler = RateLimiterHandler()

        starting_page = self.state.determine_state().get("next_page_url", 0)
        log.info(
            f"Initializing Extractor...\n"
            f"Last saved page: {starting_page}\n"
            f"Starting URL: {self.state.determine_state().get('next_page_url', 0)}"
        )

    def make_requests(self) -> Dict:

        while self.last_saved_page < 5:  # test volume
            current_page = self.last_saved_page + 1
            #current page is only used for logging within the namespace of this function, and
            # not for tracking progress. progress is tracked by self.last_saved_page
            next_page_token = self.next_page_url
            try:
                log.info(f"Starting from page {current_page}")

                self.rate_limit_handler.wait_if_needed()

                for attempt in range(1, self.max_retries + 1):
                    response = requests.get(self.next_page_url, timeout=self.timeout)

                    if response.status_code == 200:
                        data = response.json()
                        next_page_token = data.get("nextPageToken")
                        self.save_response(self.last_saved_page, data, self.execution_date)


                        #at this point, save_response would have incremented last saved page by 1
                        self.state.save_checkpoint(self.last_saved_page, next_page_token)

                        break

                    elif attempt >= self.max_retries and response.status_code != 200:
                        log.error(
                            f"Request exception FAILED AFTER {self.max_retries} attempts on page {current_page}"
                        )

                        persist_extraction_state_before_failure(
                            error=RequestTimeoutError,
                            context=self.context,
                            metadata={
                                "pages_loaded": self.last_saved_page,
                                "next_page_url": self.next_page_url,
                                "date": self.execution_date,
                            }
                        )

                if not next_page_token:
                    # consider where this could be as a result of errors, and not the extractor reaching the last page
                    log.info(f"Next page not found on page {current_page}")

                    metadata = {
                        "pages_loaded": self.last_saved_page,
                        "last_saved_token": self.state.determine_state().get("last_saved_token"), #using last known state
                        "token_type": "Last known due to inability to extract token from last saved page",
                        "date": self.execution_date,
                    }
                    #notify here

                    return metadata

                # stress test
                # if self.current_page == 3:
                #     self.failure_generator.maybe_fail_extraction(self.current_page)

                self.state.last_saved_token = next_page_token

                log.info(f"Successfully made request to page {self.last_saved_page}")

            except Exception as e:
                persist_extraction_state_before_failure(
                    error=e,
                    context=self.context,
                    metadata={
                        "pages_loaded": self.last_saved_page,
                        "last_saved_token": next_page_token,
                        "date": self.execution_date,
                    }
                )

        metadata = {
            "pages_loaded": self.last_saved_page,
            "last_saved_token": self.state.last_saved_token,
            "date": self.execution_date,
            "location": ""
        }
        return metadata


    def save_response(self, current_page: int, data: Dict, bucket_destination: str) -> str:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        bucket_name = config.CTGOV_STAGING_BUCKET
        key = f"{bucket_destination}/{current_page}.parquet"

        self.s3.load_bytes(
            bytes_data=buffer.getvalue(), key=key, bucket_name=bucket_name, replace=True
        )
        self.last_saved_page += 1

        log.info(f"Successfully saved page {self.last_saved_page} at s3://{key}")

        return key

