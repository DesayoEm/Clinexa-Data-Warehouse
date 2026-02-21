import logging
import json
from typing import Dict
import pandas as pd
from io import StringIO, BytesIO
from include.etl.load.pk_map import PK_MAP
from airflow.utils.context import Context
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config.env_config import config


class Loader:
    def __init__(self, context: Context, s3_hook: S3Hook = None):
        self.context = context
        self.execution_date = context.get("ds")
        self.log = logging.getLogger("airflow.task")
        self.s3 = s3_hook or S3Hook(aws_conn_id="aws_airflow")
        self.pg_conn = PostgresHook(postgres_conn_id="clinexa_db")

    def mark_checkpoint(self, checkpoint_value) -> None:
        """
        Persist progress marker to enable resumption after failure.
        Creates a multi level index of the folder and file last processed to skip already-processed files on retry.

        Args:
            checkpoint_value: The value of the checkpoint to be marked

        """

        ti = self.context.get("task_instance")
        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        Variable.set(checkpoint_key, json.dumps(checkpoint_value))
        # self.log.info(
        #     f"Checkpoint saved - Key: {checkpoint_key}, Checkpoint: {checkpoint_value}"
        # ) #hella noisy

    def load_checkpoint(self) -> Dict:
        """
        Load checkpoint for resumable folder/file processing.

        Checkpoint structure:
            processed_folders: set of fully completed folder names
            last_processed_index: file index to resume from within the
                first unprocessed folder (determined by sort order)

        Both folder and file lists are sorted to ensure deterministic
        ordering across retries.
        """
        self.log.info("Determining starting point for loader...")
        default_state = {"last_processed_index": 0, "last_processed_key": None}

        ti = self.context.get("task_instance")
        if not ti:
            self.log.warning("No task instance found in context, starting fresh")
            return default_state

        self.log.info(f"Current try_number: {ti.try_number}")
        if ti.try_number == 1:
            self.log.info("First run. Starting fresh load")
            return default_state

        checkpoint_key = f"{ti.task_id}_{self.execution_date}"

        try:
            checkpoint_json = Variable.get(checkpoint_key)
            checkpoint = json.loads(checkpoint_json)
            last_processed_index = checkpoint.get("last_processed_index")
            last_processed_key = checkpoint.get("last_processed_key")

            self.log.info(
                f"Checkpoint loaded - Key: {checkpoint_key}, INDEX: {last_processed_index}, LAST PROCESSED KEY: {last_processed_key}"
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

    def load_from_datalake(self):
        """
        Load parquet files from S3 staging layer into Postgres with upsert semantics.

        Reads transformed parquet files from the datalake, loads each into a temp table,
        then upserts into the corresponding staging table. Preserves first_loaded_on for
        existing rows while updating last_seen_on, enabling downstream SCD Type 2 detection.

        Process:
            1. List and sort all parquet files for the execution date
            2. Resume from last checkpoint if retrying after failure
            3. For each file:
               - Load parquet from S3 into DataFrame
               - COPY into temp table
               - Upsert into staging with ON CONFLICT
               - Checkpoint progress for resumability

        FK constraints are disabled during load (session_replication_role = 'replica')
        since the transformation layer guarantees referential integrity.

        Checkpoint:
            Uses sorted file list + index for deterministic resume. On retry,
            skips to last_processed_index + 1 and continues from there.

        Raises:
            Exception: Re-raises any database errors after resetting session_replication_role
        """

        bucket_name = config.CLINEXA_BUCKET
        prefix = f"{config.CTGOV_DEST}/{config.STAGING_DEST}/{self.execution_date}/"

        checkpoints = self.load_checkpoint()

        files = self.s3.list_keys(bucket_name=bucket_name, prefix=prefix)
        files = sorted(f for f in files if "manifest" not in f)

        files_to_load = len(files)
        self.log.info(f"Found {files_to_load} files to load")

        last_processed_index = checkpoints.get("last_processed_index")
        start_index = last_processed_index + 1 if last_processed_index else 0

        files_left = files_to_load - start_index
        self.log.info(
            f"Found {files_left} files left to load after reading  checkpoint"
        )

        if not files_left:
            self.log.info("No files left to load")
            return

        conn = self.pg_conn.get_conn()
        with conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
            try:
                for index, file_key in enumerate(
                    files[start_index:], start=start_index
                ):
                    obj = self.s3.get_key(file_key, bucket_name=bucket_name)
                    buffer = BytesIO()
                    obj.download_fileobj(buffer)
                    buffer.seek(0)
                    df = pd.read_parquet(buffer)

                    if df.empty or len(df.columns) == 0:
                        # Ideally the transformation layer should not save empty files but JIC
                        self.log.info(f"Skipping empty file: {file_key}")
                        continue

                    table_name = file_key.split("/")[
                        -2
                    ]  # files are saved in datalake with their corresponding table names

                    audit_cols = {"first_loaded_on", "last_seen_on"}
                    cols = [c for c in df.columns if c not in audit_cols]

                    csv_buffer = StringIO()
                    df[cols].to_csv(csv_buffer, index=False, header=False)
                    csv_buffer.seek(0)

                    cur.execute(
                        f"CREATE TEMP TABLE tmp_{table_name} (LIKE staging.{table_name} INCLUDING DEFAULTS)"
                    )

                    cur.copy_expert(
                        f"COPY tmp_{table_name} ({','.join(cols)}) FROM STDIN WITH CSV",
                        csv_buffer,
                    )

                    pk_cols = PK_MAP[table_name]
                    update_cols = [c for c in cols if c not in pk_cols]
                    update_set = ",\n".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

                    cur.execute(
                        f"""
                        INSERT INTO staging.{table_name} ({','.join(cols)}, first_loaded_on, last_seen_on)
                        SELECT {','.join(cols)}, DATE '{self.execution_date}', DATE '{self.execution_date}'
                        FROM tmp_{table_name}
                        ON CONFLICT ({','.join(pk_cols)}) 
                        DO UPDATE SET
                            last_seen_on = DATE '{self.execution_date}',
                            {update_set}
                    """
                    )

                    cur.execute(f"DROP TABLE tmp_{table_name}")
                    conn.commit()
                    self.mark_checkpoint(
                        {"last_processed_index": index, "last_processed_key": file_key}
                    )

            finally:
                cur.execute("SET session_replication_role = 'origin';")
                self.log.info("Finished loading files")
