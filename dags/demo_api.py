import sys
from datetime import datetime

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")
import api_ingestion as api

MOIS = "2022-03"


@dag(
    dag_id="demo_api",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "token_invalide": Param(False, type="boolean",
                                description="Coche pour envoyer un mauvais token a l'API"),
    },
    default_args={"owner": "noura", "retries": 0},
)
def demo_api():

    @task
    def ingerer(params=None):
        token = "mauvais-token" if params["token_invalide"] else api.TOKEN_VALIDE
        return api.ingerer_api(token=token)

    @task
    def enrichir(info):
        return api.enrichir_evenements(info, MOIS)

    enrichir(ingerer())


demo_api()