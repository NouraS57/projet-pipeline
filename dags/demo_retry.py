import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/src")
import cyber_pipeline as cp

MOIS = "2022-03"


@dag(
    dag_id="demo_retry",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={
        "owner": "noura",
        "retries": 3,                           # jusqu'a 3 nouvelles tentatives
        "retry_delay": timedelta(seconds=10),   # attente entre deux essais
        "execution_timeout": timedelta(minutes=5),
    },
)
def demo_retry():

    @task
    def extract_instable(ti=None):
        """Simule une source instable : echoue aux essais 1 et 2, reussit au 3e."""
        essai = ti.try_number
        if essai < 3:
            raise ConnectionError(f"Source indisponible (essai {essai}) - erreur transitoire")
        return cp.extract(MOIS)

    @task
    def validate(info):
        return cp.validate(info)

    @task
    def transform(info):
        return cp.transform(info)

    @task
    def load(info):
        return cp.load(info)

    load(transform(validate(extract_instable())))


demo_retry()