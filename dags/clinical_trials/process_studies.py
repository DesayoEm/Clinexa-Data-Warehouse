from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.context import get_current_context
from include.etl.extraction.extraction import Extractor
from include.etl.transformation.orchestration import Transformer
from include.etl.load.load import Loader


@dag(
    dag_id="process_studies",
    start_date=datetime(2026, 2, 16),
    catchup=False,
    schedule=None,
    tags=["ctgov"],
    template_searchpath=["/opt/airflow/include"],
)
def process_ct_gov():

    @task
    def extract_ctgov():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        e = Extractor(context=context, s3_hook=s3_hook)

        return e.make_requests()

    @task
    def transform_ctgov():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        t = Transformer(context=context, s3_hook=s3_hook)
        return t.transform_studies_batch()

    @task
    def load_ctgov():
        context = get_current_context()
        s3_hook = S3Hook(aws_conn_id="aws_airflow")

        l = Loader(context=context, s3_hook=s3_hook)
        return l.load_from_datalake()

    create_schemas_and_tables = SQLExecuteQueryOperator(
        task_id="create_schemas_and_tables",
        sql="/etl/load/ddl.sql",
        conn_id="clinexa_db",
    )

    extract_task = extract_ctgov()
    transform_task = transform_ctgov()
    setup = create_schemas_and_tables
    load_task = load_ctgov()

    setup
    extract_task >> transform_task >> load_task


process_ct_gov()
