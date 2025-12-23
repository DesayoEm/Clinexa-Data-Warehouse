from typing import Tuple
import io
import logging
import json
from datetime import datetime
import pandas as pd
import hashlib

from data_cleaning import Cleaner
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class Transformer:
    def __init__(self, context: Context, s3_dest_hook: S3Hook = None):
        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger("airflow.task")

        self.s3 = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow")
        self.cleaner = Cleaner()
        self.problematic_records: pd.DataFrame = pd.DataFrame()
        self.batch_size: int = 100000
        self.duplicate_count: int = 0

    @staticmethod
    def generate_key(*args) -> str:
        """Generates a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reusable column standardization"""
        df.columns = [self.cleaner.standardize_column_name(col) for col in df.columns]
        return df

    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Reusable deduplication"""
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            df.drop_duplicates(inplace=True)

        return df, duplicate_count

    @staticmethod
    def add_key_as_first_column(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """Move key column to first position"""
        return df[[key_col] + [col for col in df.columns if col != key_col]]

    def transform_entity(self, entity_data_location: str, entity_type: str):
        """Transform and load at once or in batches"""
        try:
            entity_df = pd.read_parquet(entity_data_location)
        except Exception as e:
            raise
