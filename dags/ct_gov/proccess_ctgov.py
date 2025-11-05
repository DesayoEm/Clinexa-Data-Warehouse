from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from ct_gov.include.etl.extract import Extractor
from ct_gov.include.etl.setup import Setup
from ct_gov.include.config import config
from ct_gov.include.handlers.s3_handler import S3Handler

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


@dag(
    dag_id="process_ct_gov",
    start_date=datetime(2025, 10, 27),
    catchup=False,
    schedule=None,
    tags=["ctgov"]
)


def process_ct_gov():

    @task
    def extract():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow_user")

        e = Extractor(context=context, s3_hook=s3_hook)

        return e.make_requests()


    extract_task = extract()


process_ct_gov()