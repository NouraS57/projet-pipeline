import logging
import sys
from datetime import datetime

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")
import streaming as st

log = logging.getLogger(__name__)
MOIS = "2022-03"


@dag(
    dag_id="demo_streaming",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "nb_evenements": Param(300, type="integer",
                               description="Nombre d'evenements du flux a traiter"),
    },
    default_args={"owner": "noura", "retries": 0},
)
def demo_streaming():

    @task
    def traiter_flux(params=None):
        return st.lancer_streaming(MOIS, int(params["nb_evenements"]))

    @task
    def comparer_avec_batch(res):
        log.info("STREAMING : premiere alerte disponible apres %s s", res["premiere_alerte_s"])
        log.info("BATCH : toutes les alertes seraient disponibles apres %s s (fin du lot)",
                 res["duree_totale_s"])
        return res

    comparer_avec_batch(traiter_flux())


demo_streaming()