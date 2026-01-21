from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction.extraction import Extractor
from include.etl.transformation.orchestration import Transformer


@dag(
    dag_id="process_studies",
    start_date=datetime(2026, 1, 19),
    catchup=False,
    schedule=None,
    tags=["ctgov"],
)
def process_ct_gov():

    @task
    def extract_ctgov():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        e = Extractor(context=context, s3_hook=s3_hook)

        return e.make_requests()

    @task
    def def_transform_ctgov():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        t = Transformer(context=context, s3_hook=s3_hook)
        return t.transform_studies_batch()

    extract_task = extract_ctgov()
    transform_task = def_transform_ctgov()

    extract_task >> transform_task


process_ct_gov()
