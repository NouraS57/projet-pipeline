import logging
import sys
import time
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")

log = logging.getLogger(__name__)


@dag(
    dag_id="demo_timeout",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "duree": Param(60, type="integer", description="Duree du traitement simule, en secondes"),
    },
    default_args={
        "owner": "noura",
        "retries": 0,  # pas de nouvel essai : on veut voir le timeout seul
    },
)
def demo_timeout():

    @task(execution_timeout=timedelta(seconds=15))  # limite : 15 secondes
    def traitement_lent(params=None):
        """Simule un traitement qui dure `duree` secondes."""
        duree = int(params["duree"])
        log.info("traitement_lent : debut, duree prevue = %s s (limite = 15 s)", duree)
        for seconde in range(1, duree + 1):
            time.sleep(1)
            if seconde % 5 == 0:
                log.info("traitement_lent : %s s ecoulees", seconde)
        log.info("traitement_lent : termine dans les temps")
        return duree

    @task
    def tache_suivante(duree):
        log.info("tache_suivante : executee, le traitement a dure %s s", duree)

    tache_suivante(traitement_lent())


demo_timeout()