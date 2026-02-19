import logging
import json
from typing import Dict
import pandas as pd
from io import StringIO, BytesIO
from include.etl.load.pk_map import PK_MAP
from airflow.utils.context import Context
from airflow.models import Variable
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
        self.log.info(
            f"Checkpoint saved - Key: {checkpoint_key}, Checkpoint: {checkpoint_value}"
        )

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
        bucket_name = config.CLINEXA_BUCKET
        prefix = f"{config.CTGOV_DEST}/{config.STAGING_DEST}/{self.execution_date}/"

        checkpoints = self.load_checkpoint()

        files = self.s3.list_keys(bucket_name=bucket_name, prefix=prefix)
        files = sorted(f for f in files if "manifest" not in f)

        last_processed_index = checkpoints.get("last_processed_index")
        start_index = last_processed_index + 1 if last_processed_index else 0

        conn = self.pg_conn.get_conn()
        with conn.cursor() as cur:
            try:
                for index, file_key in enumerate(
                    files[start_index:], start=start_index
                ):
                    cur.execute("SET session_replication_role = 'replica';")

                    obj = self.s3.get_key(file_key, bucket_name=bucket_name)
                    buffer = BytesIO()
                    obj.download_fileobj(buffer)
                    buffer.seek(0)
                    df = pd.read_parquet(buffer)

                    if df.empty or len(df.columns) == 0:
                        self.log.info(f"Skipping empty file: {file_key}")
                        continue

                    table_name = file_key.split("/")[-2]

                    audit_cols = {"first_loaded_at", "last_seen_at"}
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
                        INSERT INTO staging.{table_name} ({','.join(cols)}, first_loaded_at, last_seen_at)
                        SELECT {','.join(cols)}, NOW(), NOW()
                        FROM tmp_{table_name}
                        ON CONFLICT ({','.join(pk_cols)}) 
                        DO UPDATE SET
                            last_seen_at = NOW(),
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
