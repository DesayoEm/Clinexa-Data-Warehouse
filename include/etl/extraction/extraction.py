import requests
import logging
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pandas as pd
from typing import Dict
import json
import time

from airflow.models import Variable
from airflow.utils.context import Context

from include.monitoring.exceptions import RequestExhaustionError
from config.env_config import config


class StateHandler:
    """
    Manages extraction state persistence and recovery for clinical trials data extraction.

    This stateless utility class handles checkpoint operations using Airflow Variables,
    enabling extraction jobs to resume from their last successful point after failures or retries.
    State is stored per task_id and execution_date combination, ensuring isolation between
    different DAG runs.

    The state includes:
    - last_saved_page: The page number that was successfully saved
    - last_saved_token: The pagination token for the next page
    - next_page_url: The constructed URL for the next API request
    - previous_token: The token used for the current page (for verification)

    Attributes:
        context (Context): Airflow task context containing execution metadata
        execution_date (str): The logical date of the DAG run (format: YYYY-MM-DD)
        log (logging.Logger): Airflow task logger for tracking state operations
    """

    def __init__(self, context: Context):
        """
        Initialize the StateHandler with Airflow task context.
        Args:
            context (Context): Airflow task context containing execution metadata,
            task instance, and other runtime information
        """
        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger("airflow.task")

    def determine_state(self) -> Dict:
        """
        Determine the starting point for data extraction by checking for saved checkpoints.

        This method implements the recovery logic for failed or retried tasks:
        - On first run (try_number == 1): Returns default state to start fresh
        - On retry attempts: Attempts to load checkpoint from previous run
        - On checkpoint errors: Falls back to default state with appropriate logging

        The checkpoint key is constructed as: {task_id}_{execution_date}

        Returns:
            Dict: State dictionary containing:
                - last_saved_page (int): Page number of last successful save (0 for fresh start)
                - last_saved_token (str|None): Pagination token for next page
                - next_page_url (str): Full URL for next API request
                - previous_token (str|None): Token from previous page (for verification)

        Note:
            - Handles missing checkpoints by starting fresh
            - Logs checkpoint loading failures for debugging
            - JSON parsing errors are caught and logged with full JSON data
        """

        self.log.info("Determining starting point for extractor...")
        default_state = {
            "last_saved_page": 0,
            "last_saved_token": None,
            "next_page_url": config.FIRST_PAGE_URL,
            "previous_token": None,
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

    def save_checkpoint(
        self, previous_token: str, last_saved_page: int, last_saved_token: str
    ) -> None:
        """
        Persist current extraction state to Airflow Variables for retry recovery.

        Saves a checkpoint that allows the extraction process to resume from its current
        position if the task fails or is retried. The checkpoint is stored as a JSON string
        in an Airflow Variable, using a composite key of task_id and execution_date.

        This method overwrites any previous checkpoint for the same task_id + execution_date
        combination, ensuring only the most recent state is preserved, and the Metadata db is not bloated

        Args:
            previous_token (str): The pagination token that was used for the current page.
                                Used for verification and potential rollback scenarios.
            last_saved_page (int): The page number that was successfully saved to S3.
                                 Next extraction will start from last_saved_page + 1.
            last_saved_token (str): The pagination token for the next page to be fetched.
                                  Used to construct the next_page_url.

        Returns:
            None

        Side Effects:
            - Creates or updates an Airflow Variable with key: {task_id}_{execution_date}
            - Logs checkpoint details including page number and tokens

        Note:
            Checkpoints are saved:
            - After each successful page save
            - Before raising exceptions on failures
            - At the end of successful extraction runs
        """

        ti = self.context.get("task_instance")
        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        checkpoint_value = {
            "last_saved_page": last_saved_page,
            "last_saved_token": last_saved_token,
            "next_page_url": f"{config.BASE_URL}{last_saved_token}",
            "previous_token": previous_token,
        }

        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        self.log.info(
            f"Checkpoint saved - Key: {checkpoint_key}, Page: {last_saved_page}, Previous token: {previous_token}, Current token: {last_saved_token}"
        )


class Extractor:
    """
    Orchestrates paginated API extraction of clinical trials data with fault tolerance.

    This class implements is an  extraction pipeline that:
    - Fetches paginated data from the Clinical Trials API
    - Converts responses to Parquet format for efficient storage
    - Saves data incrementally to S3 with page-level granularity
    - Handles rate limiting
    - Maintains checkpoints for recovery from failures
    - Generates extraction manifests for downstream processing

    The extractor follows a stateful pagination pattern, tracking:
    - Current page number and pagination tokens
    - Previous token (for verification and rollback)
    - Next page URL construction

    Flow:
    1. Initialize with context and determine starting state
    2. Loop through paginated API responses
    3. Convert each response to Parquet format
    4. Save to S3 with incremental page numbers
    5. Update checkpoints after each successful save
    6. Generate manifest on completion

    Attributes:
        context (Context): Airflow task context for execution metadata
         execution_date (str): Logical date of the DAG run
         log (logging.Logger): Airflow task logger
         state (StateHandler): Handler for checkpoint operations
         timeout (int): HTTP request timeout in seconds
         max_retries (int): Maximum retry attempts per page
         last_saved_page (int): Counter for successfully saved pages
         next_page_url (str|None): URL for the next API request
         last_saved_token (str|None): Current pagination token
         previous_token (str|None): Previous pagination token (for verification)
         max_requests (int): Maximum requests allowed in the time window (default: 50)
         window (int): Time window in seconds for rate limiting (default: 60)
         requests (list): Timestamps of recent requests for rate limiting
         s3_hook: S3 connection hook for file operations


    Raises:
        RequestExhaustionError: When max_retries is exceeded for a page
        Exception: Various exceptions from HTTP requests or S3 operations
    """

    def __init__(
        self, context: Context, s3_hook, timeout: int = 30, max_retries: int = 3
    ):

        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger("airflow.task")
        self.state = StateHandler(self.context)
        self.timeout: int = timeout

        self.max_retries: int = max_retries
        self.last_saved_page: int = 0
        self.next_page_url: str | None = None
        self.last_saved_token: str | None = None
        self.previous_token: str | None = None

        self.max_requests: int = 50
        self.window: int = 60
        self.requests = []

        initial_state = self.state.determine_state()
        self.last_saved_page = initial_state.get("last_saved_page")
        self.next_page_url = initial_state.get("next_page_url")

        self.s3_hook = s3_hook

        self.log.info(
            f"Initializing Extractor...\n"
            f"Last saved page: {self.last_saved_page}\n"
            f"Starting URL: {self.next_page_url}"
        )

    def wait_if_needed(self):
        """
        Implements a sliding window rate limiter that allows a maximum of 50 requests
        per 60-second window. (standard for clinicaltrials.gov)

        Algorithm:
        1. Remove timestamps older than the current window (60 seconds)
        2. If at max capacity (50 requests), calculate required sleep time
        3. Sleep until the oldest request falls outside the window
        4. Clear request history and record current request timestamp

        Returns:
            None

        Side Effects:
            - Modifies self.requests list by pruning old timestamps
            - May sleep the current thread if rate limit is reached
            - Appends current timestamp to self.requests

        """
        now = time.time()

        # remove timestamps outside current window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.window
        ]

        if len(self.requests) >= self.max_requests:
            sleep_time = self.window - (now - self.requests[0])
            time.sleep(sleep_time)
            self.requests = []

        self.requests.append(time.time())

    def make_requests(self) -> Dict:
        """
        main extraction loop with pagination, retry logic, and fault tolerance.

         Request Flow Per Page:
         - Apply rate limiting delay (if needed)
         - Attempt HTTP GET with retries
         - On success: parse JSON, save to S3, update tokens, continue
         - On failure after retries: save checkpoint, push failure metadata, raise error
         - On no next token: save final checkpoint, generate manifest, return metadata

         Checkpoint saving behaviour:
         - Checkpoints saved before raising exceptions
         - Checkpoints saved at successful completion

         Returns:
             Dict: Extraction metadata containing:
                 - pages_extracted (int): Total number of pages successfully saved
                 - last_valid_token (str): Token of the second-to-last page (for verification)
                 - final_token (str|None): Final pagination token (should be None on completion)
                 - data_location (str): S3 path to extracted data

         Raises:
             RequestExhaustionError: When a page fails after max_retries attempts.
                 Checkpoint is saved before raising, allowing recovery.
                 Failure metadata is pushed to XCom for notifications.

         Note:
             - current_page is used for logging only; progress tracking uses last_saved_page
             - The infinite while loop breaks when no next_page_token is found
             - Previous token tracking enables verification that all data was extracted
             - Rate limiting is applied before each request to prevent API throttling
        """

        while True:  # test volume
            current_page = self.last_saved_page + 1
            # current page is used for logging and error reporting within the namespace of this function, and
            # not for tracking progress. progress is tracked by self.last_saved_page
            next_page_token = None

            try:
                self.log.info(f"Starting from page {current_page}")

                self.wait_if_needed()

                for attempt in range(1, self.max_retries + 1):
                    response = requests.get(self.next_page_url, timeout=self.timeout)

                    if response.status_code == 200:
                        self.log.info(
                            f"Successfully made request to page {current_page}"
                        )
                        data = response.json()
                        next_page_token = data.get("nextPageToken")

                        self.previous_token = self.last_saved_token
                        self.last_saved_token = next_page_token

                        self.save_response(current_page, data)
                        self.next_page_url = f"{config.BASE_URL}{self.last_saved_token}"
                        break

                    elif attempt >= self.max_retries and response.status_code != 200:

                        self.state.save_checkpoint(
                            self.previous_token,
                            self.last_saved_page,
                            self.last_saved_token,
                        )

                        ti = self.context["task_instance"]

                        # construct failure metadata for notifier
                        failure_metadata = {
                            "status": "failed",
                            "pages_extracted": self.last_saved_page,
                            "last_valid_token": self.previous_token,
                            "error_type": RequestExhaustionError,
                            "error_message": f"Failed to fetch page {current_page} after {self.max_retries} attempts. URL: {self.next_page_url}",
                        }

                        ti.xcom_push(key="metadata", value=failure_metadata)

                        # raise for logging
                        raise RequestExhaustionError(
                            page_number=current_page,
                            max_attempts=self.max_retries,
                            url=self.next_page_url,
                        )

                if not next_page_token:
                    # All pages extracted.
                    # tracking self.previous_token is important here. Saving second to last page
                    # is useful to verify if all data was truly extracted

                    self.log.info(f"Next page not found on page {current_page}")

                    self.state.save_checkpoint(
                        self.previous_token, self.last_saved_page, self.last_saved_token
                    )

                    manifest = {
                        "location": f"s3://{config.CTGOV_BUCKET}/{self.execution_date}",
                        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "metrics": {
                            "page_count": self.last_saved_page,
                        },
                        "lineage": {
                            "dag_id": self.context["dag"].dag_id,
                            "run_id": self.context["run_id"],
                            "execution_date": self.execution_date,
                        },
                    }

                    manifest_key = f"{self.execution_date}_manifest.json"

                    self.s3_hook.load_string(
                        string_data=json.dumps(manifest, indent=2),
                        key=manifest_key,
                        bucket_name=config.CTGOV_BUCKET,
                        replace=True,
                    )

                    self.log.info(
                        f"Manifest saved to s3://{config.CTGOV_BUCKET}/{self.execution_date}/{manifest_key}"
                    )

                    metadata = {
                        "pages_extracted": self.last_saved_page,
                        "last_valid_token": self.previous_token,
                        "final_token": self.last_saved_token,
                        "data_location": f"s3://{config.CTGOV_BUCKET}/{self.execution_date}/",
                    }
                    return metadata

            except Exception as e:
                self.log.info(f"{str(e)}")

                self.state.save_checkpoint(
                    self.previous_token, self.last_saved_page, self.last_saved_token
                )

                ti = self.context["task_instance"]

                # construct failure metadata for notifier
                failure_metadata = {
                    "status": "failed",
                    "pages_extracted": self.last_saved_page,
                    "last_valid_token": self.previous_token,
                    "error_type": RequestExhaustionError,
                    "error_message": f"Failed to fetch page {current_page} after {self.max_retries} attempts. URL: {self.next_page_url}",
                }

                ti.xcom_push(key="metadata", value=failure_metadata)
                # raise for logging
                raise RequestExhaustionError(
                    page_number=current_page,
                    max_attempts=self.max_retries,
                    url=self.next_page_url,
                )

    def save_response(self, page_number: int, data: Dict) -> None:
        """
        Convert API response to Parquet format and persist to S3.

        This method handles the transformation and storage of a single page of API data:
        1. Converts JSON response dict to pandas DataFrame
        2. Transforms DataFrame to PyArrow Table for efficient Parquet encoding
        3. Writes Parquet data to in-memory buffer
        4. Uploads buffer contents to S3 with structured key naming
        5. Increments page counter after successful save

        Args:
            page_number (int): The logical page number for this data chunk.
                Used for file naming and progress tracking.
                Should match current_page from calling context.
            data (Dict): JSON response data from the API. Should be a dictionary
                that can be converted to a pandas DataFrame.

        Returns:
            None

        Side Effects:
            - Creates a Parquet file in S3
            - Increments self.last_saved_page counter
            - Logs successful save with S3 destination path

        Note:
            - Uses in-memory buffer to avoid disk I/O
            - Replaces existing file if page is re-processed
            - Page counter incremented only after successful S3 upload
            - Destination path logged for debugging and verification
        """

        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        bucket = config.CTGOV_BUCKET
        key = f"{self.execution_date}/{page_number}.parquet"

        self.s3_hook.load_bytes(
            bytes_data=buffer.getvalue(), key=key, bucket_name=bucket, replace=True
        )

        self.last_saved_page += 1

        destination = f"s3://{bucket}/{key}" if key else "Unknown"
        self.log.info(
            f"Successfully saved page {self.last_saved_page} at {destination}"
        )
