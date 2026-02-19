import logging
import json
import io
from typing import List, Dict
from collections import defaultdict
import pandas as pd

from airflow.utils.context import Context
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.etl.transformation.core_transformation.study_transformation import (
    process_study_file,
    post_process_tables,
)
from config.env_config import config
from include.etl.transformation.models import StudyResult

EXPECTED_TABLES = StudyResult.expected_tables()


class Transformer:
    """
    Manages the end-to-end transformation workflow: iterating through raw
    parquet files in S3, transforming each into normalised  records,
    merging results, and handling failures with checkpoint recovery.

    Designed to run within an Airflow task context, using the execution date
    for partitioning and S3Hook for file access.

    Attributes:
        context: Airflow task context providing execution metadata.
        execution_date: Logical date of the DAG run (ds), used for partitioning.
        log: Airflow task logger for structured logging.
        s3: S3Hook instance for reading source files and writing checkpoints.
    """

    def __init__(self, context: Context, s3_hook: S3Hook = None):
        self.context = context
        self.execution_date = context.get("ds")
        self.log = logging.getLogger("airflow.task")
        self.s3 = s3_hook or S3Hook(aws_conn_id="aws_airflow")

    def mark_checkpoint(self, index, file):
        """
        Persist progress marker to enable resumption after failure.

        Saves the index and file path of the last attempted file, allowing
        transform_studies_batch to skip already-processed files on retry.

        Args:
            index: Position in the file list where processing failed.
            file: S3 path of the file that failed.
        """
        ti = self.context.get("task_instance")
        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        checkpoint_value = {
            "last_processed_index": index,
            "last_processed_key": file,
        }

        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        self.log.info(
            f"Checkpoint saved - Key: {checkpoint_key}, Index: {index}, Last processed key: {file}"
        )

    def load_checkpoint(self):
        """
        Load previously saved checkpoint to resume interrupted processing.

        Retrieves the last successful file index from S3, allowing the batch
        to skip already-processed files. Returns None or starting index if
        no checkpoint exists.
        """
        self.log.info("Determining starting point for transformer...")
        default_state = {"last_processed_index": 0, "last_processed_key": None}

        ti = self.context.get("task_instance")
        if not ti:
            self.log.warning("No task instance found in context, starting fresh")
            return default_state

        self.log.info(f"Current try_number: {ti.try_number}")
        if ti.try_number == 1:
            self.log.info("First run. Starting fresh transformation")
            return default_state

        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        try:
            checkpoint_json = Variable.get(checkpoint_key)
            checkpoint = json.loads(checkpoint_json)
            last_processed_key = checkpoint.get("last_processed_key")
            last_processed_index = checkpoint.get("last_processed_index")

            self.log.info(
                f"Checkpoint loaded - Key: {checkpoint_key}, INDEX: {last_processed_index}, LAST PROCESSED: {last_processed_key}"
            )
            self.log.info(f"Resuming from page {last_processed_index + 1}")

            return {
                "last_processed_index": last_processed_index,
                "last_processed_key": last_processed_key,
            }

        except KeyError:
            self.log.info(f"No checkpoint found for key: {checkpoint_key}")
            self.log.info(f"  Starting fresh from beginning")
            return default_state

        except json.JSONDecodeError as e:
            self.log.error(
                f"Failed to parse checkpoint JSON: {e}\n"
                f"JSON DATA\n\n"
                f"{checkpoint_json}"
            )

            self.log.info(f"Starting fresh from beginning")
            return default_state

        except Exception as e:
            self.log.info(
                f"ERROR finding checkpoint for key: {checkpoint_key} \n Error: {e}"
            )
            self.log.info(f"Defaulting to beginning")
            return default_state

    @staticmethod
    def merge_batch_results(batch_results: List[StudyResult]) -> Dict[str, List[Dict]]:
        """
        Aggregate StudyResult objects into a single dictionary of record lists.

        Combines records from multiple studies into table-keyed lists suitable
        for DataFrame conversion in `post_process_tables`.

        Args:
            batch_results: List of StudyResult objects from process_study_file.

        Returns:
            Dictionary mapping table names to lists of record dicts,
            with all studies' records concatenated per table.
        """
        merged: Dict[str, List[Dict]] = defaultdict(list)

        for study_result in batch_results:
            for table, rows in study_result.tables().items():
                merged[table].extend(rows)

        missing = set(EXPECTED_TABLES) - merged.keys()

        if missing:
            raise ValueError(f"Missing tables: {missing}")

        return merged

    def transform_studies_batch(self):
        """
        Process all raw study files for the current execution date.

        Iterates through parquet files at the given S3 location, transforming
        each file's studies
        On failure, marks a checkpoint and re-raises to trigger Airflow retry with resumption.

        Raises:
            Exception: Re-raises any processing error after checkpointing,
                preserving the original exception for Airflow visibility.
        """
        bucket_name = config.CLINEXA_BUCKET
        prefix = f"{config.CTGOV_DEST}/{config.RAW_DEST}/{self.execution_date}/"

        keys = self.s3.list_keys(bucket_name=bucket_name, prefix=prefix) or []

        files = sorted(
            key for key in keys if "manifest" not in key
        )  # sorting here to enable reliable checkpointing
        last_processed_index = self.load_checkpoint()["last_processed_index"]

        start_index = last_processed_index + 1 if last_processed_index else 0
        for index, s3_key in enumerate(files[start_index:], start=start_index):

            try:
                batch_result = process_study_file(s3_key)
                merged_batch_results = self.merge_batch_results(batch_result)
                dfs = post_process_tables(merged_batch_results)

                dfs = self.add_audit_columns(dfs)

                self.write_to_datalake(index, dfs)

                self.mark_checkpoint(index, s3_key)

            except Exception as e:
                self.log.exception(f"File failed: Exception: {str(e)}")
                raise

    def add_audit_columns(
        self, dfs: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        Add Airflow audit metadata to all DataFrames.

        Args:
            dfs: Dictionary of table_name -> DataFrame

        Returns:
            Same dictionary with audit columns added to each DataFrame
        """

        for table_name, df in dfs.items():
            if df.empty:
                continue

            df["dag_execution_date"] = self.execution_date
            df["dag_id"] = self.context["dag"].dag_id
            df["dag_run_id"] = self.context["dag_run"].run_id

        return dfs

    def write_to_datalake(self, index: int, dfs: Dict[str, pd.DataFrame]) -> None:
        bucket = config.CLINEXA_BUCKET

        for table_name, df in dfs.items():
            if df.empty or len(df.columns) == 0:
                continue

            key = (
                f"{config.CTGOV_DEST}/"
                f"{config.STAGING_DEST}/"
                f"{self.execution_date}/"
                f"{table_name}/"
                f"page-{index + 1:04d}.parquet"  # increased index by 1 to match actual page numbers
            )

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            self.s3.load_bytes(
                bytes_data=buffer.getvalue(), key=key, bucket_name=bucket, replace=True
            )
