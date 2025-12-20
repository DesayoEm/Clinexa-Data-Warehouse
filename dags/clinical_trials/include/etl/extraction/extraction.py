import requests
import logging
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pandas as pd
from typing import Dict
import json

from airflow.models import Variable
from airflow.utils.context import Context

from clinical_trials.include.handlers.rate_limit_handler import RateLimiterHandler
from clinical_trials.include.handlers.s3_handler import S3Handler
from clinical_trials.include.exceptions import RequestTimeoutError
from clinical_trials.include.config import config
from clinical_trials.include.monitoring.extractor_middleware import persist_extraction_state_before_failure


class StateHandler:
    """Stateless utility class for loading extraction state from Airflow Variables."""

    def __init__(self, context: Context):
        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger('airflow.task')

    def determine_state(self) -> Dict:
        """
        Determines where to start extraction by checking for previous state in Variables.
        Returns a new ExtractorState instance with either:
        - Checkpoint data from a previous failed run (resume from where we left off)
        - Default values (start fresh from page 0)
        """

        self.log.info("Determining starting point for extractor...")
        default_state = {
            "last_saved_page": 0,
            "last_saved_token": None,
            "next_page_url": config.FIRST_PAGE_URL,
            "previous_token": None
        }

        ti = self.context.get("task_instance")
        if not ti:
            self.log.warning("No task instance found in context, starting fresh")
            return default_state

        self.log.info(f"Current try_number: {ti.try_number}")
        if ti.try_number == 1:
            self.log.info("First run. Starting fresh extraction")
            return default_state

        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        try:
            checkpoint_json = Variable.get(checkpoint_key)
            checkpoint = json.loads(checkpoint_json)
            last_saved_page = checkpoint.get("last_saved_page")
            last_saved_token = checkpoint.get("last_saved_token")

            self.log.info(
                f"Checkpoint loaded - Key: {checkpoint_key}, Page: {last_saved_page}, Token: {last_saved_token}"
            )
            self.log.info(f"Resuming from page {last_saved_page + 1}")

            return {
                "last_saved_page": last_saved_page,
                "last_saved_token": last_saved_token,
                "next_page_url": f"{config.BASE_URL}{last_saved_token}",
            }
        except KeyError:
            self.log.info(f"No checkpoint found for key: {checkpoint_key}")
            self.log.info(f"  Starting fresh from page 0")
            return default_state

        except json.JSONDecodeError as e:
            self.log.error(
                f"Failed to parse checkpoint JSON: {e}\n"
                f"JSON DATA\n\n"
                f"{checkpoint_json}"
            )

            self.log.info(f"Starting fresh from page 0")
            return default_state

        except Exception as e:
            self.log.info(
                f"ERROR finding checkpoint for key: {checkpoint_key} \n Error: {e}"
            )
            self.log.info(f"Defaulting to 0")
            return default_state

    def save_checkpoint(self, previous_token: str, last_saved_page: int, last_saved_token: str) -> None:
        """
        Save checkpoint to Airflow Variable for retry recovery.
        Overwrites previous run for same task_id + execution_date.
        """
        ti = self.context.get("task_instance")
        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        checkpoint_value = {
            "last_saved_page": last_saved_page,
            "last_saved_token": last_saved_token,
            "next_page_url": f"{config.BASE_URL}{last_saved_token}",
            "previous_token": previous_token
        }

        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        self.log.info(
            f"Checkpoint saved - Key: {checkpoint_key}, Page: {last_saved_page}, Previous token: {previous_token}, Current token: {last_saved_token}"
        )


class Extractor:
    def __init__(
        self, context: Context, s3_hook, timeout: int = 30, max_retries: int = 3
    ):

        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger('airflow.task')
        self.state = StateHandler(self.context)
        self.timeout: int = timeout
        self.max_retries: int = max_retries
        self.last_saved_page: int = 0
        self.next_page_url: str | None = None
        self.last_saved_token: str | None = None
        self.previous_token: str | None = None

        initial_state = self.state.determine_state()
        self.last_saved_page = initial_state.get("last_saved_page")
        self.next_page_url = initial_state.get("next_page_url")

        self.s3_hook = s3_hook
        self.rate_limit_handler = RateLimiterHandler()

        self.log.info(
            f"Initializing Extractor...\n"
            f"Last saved page: {self.last_saved_page}\n"
            f"Starting URL: {self.next_page_url}"
        )

    def make_requests(self) -> Dict:

        while True:  # test volume
            current_page = self.last_saved_page + 1
            # current page is used for logging and error reporting within the namespace of this function, and
            # not for tracking progress. progress is tracked by self.last_saved_page
            next_page_token = None

            try:
                self.log.info(f"Starting from page {current_page}")

                self.rate_limit_handler.wait_if_needed()

                for attempt in range(1, self.max_retries + 1):
                    response = requests.get(self.next_page_url, timeout=self.timeout)

                    if response.status_code == 200:
                        self.log.info(f"Successfully made request to page {current_page}")
                        data = response.json()
                        next_page_token = data.get("nextPageToken")

                        self.previous_token = self.last_saved_token
                        self.last_saved_token = next_page_token

                        self.save_response(current_page, data)
                        self.next_page_url = f"{config.BASE_URL}{self.last_saved_token}"
                        break

                    elif attempt >= self.max_retries and response.status_code != 200:
                        self.log.error(
                            f"Request exception FAILED AFTER {self.max_retries} attempts on page {current_page}"
                        )

                        self.state.save_checkpoint(
                            self.previous_token, self.last_saved_page,  self.last_saved_token
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
                    #All pages extracted.
                    # tracking self.previous_token is important here. Saving second to last page
                    # is useful to verify if all data was truly extracted

                    self.log.info(f"Next page not found on page {current_page}")

                    self.state.save_checkpoint(
                        self.previous_token, self.last_saved_page, self.last_saved_token
                    )

                    manifest = {
                        "location": f"s3://{config.CTGOV_STAGING_BUCKET}/{self.execution_date}",
                        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "metrics": {
                            "page_count": self.last_saved_page,
                        },
                        "lineage": {
                            "dag_id": self.context["dag"].dag_id,
                            "run_id": self.context["run_id"],
                            "execution_date": self.execution_date,
                        }
                    }

                    manifest_key = f"{self.execution_date}_manifest.json"

                    self.s3_hook.load_string(
                        string_data=json.dumps(manifest, indent=2),
                        key=manifest_key,
                        bucket_name=config.CTGOV_STAGING_BUCKET,
                        replace=True
                    )

                    self.log.info(
                        f"Manifest saved to s3://{config.CTGOV_STAGING_BUCKET}/{self.execution_date}/{manifest_key}")

                    metadata = {
                        "pages_extracted": self.last_saved_page,
                        "last_valid_token": self.previous_token,
                        "final_token": self.last_saved_token,
                        "data_location": f"s3://{config.CTGOV_STAGING_BUCKET}/{self.execution_date}/"
                    }
                    return metadata



            except Exception as e:
                self.log.info(f"{str(e)}")

                self.state.save_checkpoint(
                    self.previous_token, self.last_saved_page, self.last_saved_token
                )

                persist_extraction_state_before_failure(
                    error=e,
                    context=self.context,
                    metadata={
                        "pages_extracted": self.last_saved_page,
                        "last_valid_token": self.previous_token,
                        "final_token": self.last_saved_token,
                        "data_location": f"s3://{config.CTGOV_STAGING_BUCKET}/{self.execution_date}/"
                    }
                )



    def save_response(self, page_number: int, data: Dict) -> None:
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        bucket = config.CTGOV_STAGING_BUCKET
        key = f"{self.execution_date}/{page_number}.parquet"

        self.s3_hook.load_bytes(
            bytes_data=buffer.getvalue(), key=key, bucket_name=bucket, replace=True
        )

        self.last_saved_page += 1

        destination = f"s3://{bucket}/{key}" if key else "Unknown"
        self.log.info(f"Successfully saved page {self.last_saved_page} at {destination}")



