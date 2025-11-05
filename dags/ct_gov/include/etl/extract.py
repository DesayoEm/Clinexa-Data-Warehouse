
import requests
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict
from dataclasses import dataclass

from ct_gov.include.config import config
from ct_gov.include.notification_middleware.extractor_middleware import (
    persist_extraction_state_before_exit, persist_extraction_state_before_failure
)

from ct_gov.include.handlers.rate_limit_handler import RateLimiterHandler
from ct_gov.include.handlers.s3_handler import S3Handler
from ct_gov.include.tests.extractor_stress_test import FailureGenerator
from ct_gov.include.exceptions import RequestTimeoutError

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


@dataclass
class ExtractorState:
    """
    Holds the state for a single extraction run.
    Each Extractor instance gets its own ExtractorState instance.
    """
    last_saved_page: int = 0
    last_saved_token: str = None
    next_page_url: str = config.FIRST_PAGE_URL

    def to_dict(self) -> Dict:
        return {
            "last_saved_page": self.last_saved_page,
            "last_saved_token": self.last_saved_token,
            "next_page_url": self.next_page_url,
        }


class StateLoader:
    """Stateless utility class for loading extraction state from Airflow XCom."""

    @staticmethod
    def load_from_context(context: Dict) -> ExtractorState:
        """
        Determines where to start extraction by checking for previous state in XCom.

        LIMITATION (for testing only):
        - If you manually clear a previously successful task, it may resume from
          the success checkpoint instead of the last failure checkpoint.
        - This does not affect production behavior as successful tasks are never retried
        #to be evaluated

        Returns a new ExtractorState instance with either:
        - Checkpoint data from a previous failed run (resume from where we left off)
        - Default values (start fresh from page 0)

        Args:
            context: Airflow context dictionary
        """

        log.info("Determining starting point for extractor...")

        ti = context.get('task_instance')
        if not ti:
            log.warning("No task instance found in context, starting fresh")
            return ExtractorState()


        #TESTING: start fresh on first attempt.
        if ti.try_number == 1:
            return ExtractorState()

        log.info(f"Current try_number: {ti.try_number}")

        current_execution_date = context.get('ds')
        states = ti.xcom_pull(
            task_ids=ti.task_id,
            key="previous_states",
            include_prior_dates=True,
        )

        if not states:
            log.info("No previous states found in XCom, starting fresh")
            return ExtractorState()


        if isinstance(states, list):
            for state in states:
                if state.get('date') == current_execution_date:
                    if (state.get('pages_loaded') is not None and
                            state.get('last_saved_token') is not None):


                        return ExtractorState(
                            last_saved_page=state['pages_loaded'],
                            last_saved_token=state['last_saved_token'],
                            next_page_url=f"{config.BASE_URL}{state['last_saved_token']}"
                        )
        else:
            state = states
            if (state.get('pages_loaded') is not None and
                    state.get('last_saved_token') is not None):

                return ExtractorState(
                    last_saved_page=state['pages_loaded'],
                    last_saved_token=state['last_saved_token'],
                    next_page_url=f"{config.BASE_URL}{state['last_saved_token']}"
                )

        log.info("No valid checkpoint found for this run, starting fresh")
        return ExtractorState()



class Extractor:
    def __init__(
            self,
            context: Dict,
            s3_hook,
            timeout: int = 30,
            max_retries: int = 3
        ):

        self.state = StateLoader.load_from_context(context)

        self.timeout = timeout
        self.max_retries = max_retries
        self.context = context

        self.s3 = s3_hook

        self.current_page = self.state.last_saved_page

        self.failure_generator = FailureGenerator(True, 0.5)
        self.rate_limit_handler = RateLimiterHandler()

        log.info(
            f"Initializing Extractor...\n"
            f"Last saved page: {self.state.last_saved_page}\n"
            f"Starting URL: {self.state.next_page_url}"
        )


    def make_requests(self):
        while self.state.last_saved_page < 10: #test volume
            try:
                self.current_page = self.state.last_saved_page + 1
                log.info(f"Starting from page {self.state.last_saved_page}")

                self.rate_limit_handler.wait_if_needed()

                for attempt in range(self.max_retries):
                    response = requests.get(self.state.next_page_url, timeout=self.timeout)

                    if response.status_code == 200:
                        data = response.json()
                        self.save_response(data, self.context["ds"])

                        next_page_token = data.get("nextPageToken")
                        self.state.last_saved_token = next_page_token
                        break

                    elif attempt >= self.max_retries -1 and response.status_code != 200:
                        log.error(
                            f"Request exception FAILED AFTER {self.max_retries} attempts on page {self.current_page}"
                        )

                        persist_extraction_state_before_failure(
                            error=RequestTimeoutError,
                            context=self.context,
                            metadata={
                                'pages_loaded': self.state.last_saved_page,
                                'last_saved_token': self.state.last_saved_token,
                                'next_page_url': self.state.next_page_url,
                                'date': self.context["ds"]
                            }

                        )


                if not next_page_token:
                    log.info(
                    f"Next page not found on page {self.current_page}"
                    f"Check metadata for the token for this page")

                    metadata = {
                        'pages_loaded': self.state.last_saved_page,
                        'last_saved_token': self.state.last_saved_token,
                        'next_page_url': self.state.next_page_url,
                        'date': self.context["ds"]
                    }
                    persist_extraction_state_before_exit(self.context, metadata)
                    return

                # stress test
                if self.current_page == 3:
                    self.failure_generator.maybe_fail_extraction(self.current_page)

                self.state.last_saved_token = next_page_token
                self.state.next_page_url = f"{config.BASE_URL}{next_page_token}"


                log.info(
                    f'Successfully made request to page {self.current_page}'
                     f'\n Next page is {self.state.next_page_url}'
                    )



            except Exception as e:
                persist_extraction_state_before_failure(
                    error = e,
                    context=self.context,
                    metadata={
                        'pages_loaded': self.state.last_saved_page,
                        'last_saved_token': self.state.last_saved_token,
                        'next_page_url': self.state.next_page_url,
                        'date': self.context["ds"]
                        },
                    )

        metadata = {
            'pages_loaded': self.state.last_saved_page,
            'last_saved_token': self.state.last_saved_token,
            'next_page_url': self.state.next_page_url,
            'date': self.context["ds"]
        }
        persist_extraction_state_before_exit(self.context, metadata)
        return


    def save_response(self, data: Dict, bucket_destination: str):
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        bucket_name = config.CTGOV_STAGING_BUCKET
        key = f"{bucket_destination}/{self.current_page}.parquet"

        self.s3.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        self.state.last_saved_page += 1

        log.info(
            f"Successfully saved page {self.state.last_saved_page} at s3://{bucket_name}/{key}"
        )
