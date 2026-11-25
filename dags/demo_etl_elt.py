import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/src")
import elt

MOIS = "2022-03"


@dag(
    dag_id="demo_etl_elt",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "noura", "retries": 0},
)
def demo_etl_elt():

    @task
    def charger_brut():
        return elt.charger_brut(MOIS)

    @task
    def transformer_sql(info):
        return elt.transformer_sql(info)

    @task
    def comparer(info):
        return elt.comparer_etl(info)

    comparer(transformer_sql(charger_brut()))


demo_etl_elt()