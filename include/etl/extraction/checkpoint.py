import logging
from typing import Dict
import json

# from airflow.models import Variable
from airflow.sdk import Variable
from airflow.utils.context import Context

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

    def mark_checkpoint(
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
