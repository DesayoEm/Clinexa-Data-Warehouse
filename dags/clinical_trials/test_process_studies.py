from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from include.tests.test_tasks import ExtractorWithFailureInjection


@dag(
    dag_id="test_process_studies",
    start_date=datetime(2025, 10, 27),
    catchup=False,
    schedule=None,
    tags=["ctgov"],
)
def process_ct_gov():

    @task
    def extract():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        e = ExtractorWithFailureInjection(context=context, s3_hook=s3_hook)

        return e.make_requests()

    extract_task = extract()


process_ct_gov()
