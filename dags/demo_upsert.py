import sys
from datetime import datetime

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")
import upsert

MOIS = "2022-03"


@dag(
    dag_id="demo_upsert",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "modifier_score": Param(False, type="boolean",
                                description="Coche pour simuler des donnees corrigees (score +1)"),
    },
    default_args={"owner": "noura", "retries": 0},
)
def demo_upsert():

    @task
    def charger_upsert(params=None):
        return upsert.upsert_mois(MOIS, modifier_score=params["modifier_score"])

    charger_upsert()


demo_upsert()