from typing import Dict
from datetime import datetime
import boto3
import os
from airflow.utils.log.logging_mixin import LoggingMixin
# from dags.ct_gov.include.config import config
from ct_gov.include.config import config


log = LoggingMixin().log


class S3Handler:
    def __init__(self, s3_client):
        self.client = s3_client

    def create_folder(self, bucket: str, folder_name: str):
       self.client.put_object(Bucket=bucket, Key=folder_name)
       return f"{bucket}/{folder_name}/"


    def upload_to_bucket(self, file_name: str, bucket: str):
        object_name = os.path.basename(file_name)
        self.client.upload_file(file_name, bucket, object_name)

