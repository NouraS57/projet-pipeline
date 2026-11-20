import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/src")
import cyber_pipeline as cp


@dag(
    dag_id="pipeline_cybersecurite",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={
        "owner": "noura",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=5),
    },
)
def pipeline_cybersecurite():

    @task
    def extract():
        return cp.extract()

    @task
    def validate(info):
        return cp.validate(info)

    @task
    def transform(info):
        return cp.transform(info)

    @task
    def load(info):
        return cp.load(info)

    load(transform(validate(extract())))


pipeline_cybersecurite()