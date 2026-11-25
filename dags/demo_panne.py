import logging
import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/src")
import cyber_pipeline as cp

log = logging.getLogger(__name__)

MOIS = "2022-03"
FLAG = "/opt/airflow/data/panne.flag"  # tant que ce fichier existe, transform plante


@dag(
    dag_id="demo_panne",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "noura", "retries": 0},  # pas de retry : on veut voir la panne
)
def demo_panne():

    @task
    def armer_panne():
        """Arme la panne simulee en creant un fichier marqueur."""
        os.makedirs(os.path.dirname(FLAG), exist_ok=True)
        with open(FLAG, "w", encoding="utf-8") as f:
            f.write("panne simulee")
        log.info("armer_panne : panne armee (fichier %s cree)", FLAG)

    @task
    def extract():
        return cp.extract(MOIS)

    @task
    def validate(info):
        return cp.validate(info)

    @task
    def transform(info):
        if os.path.exists(FLAG):
            raise RuntimeError(
                "PANNE SIMULEE dans transform : supprimer data/panne.flag, "
                "puis relancer cette tache"
            )
        return cp.transform(info)

    @task
    def load(info):
        return cp.load(info)

    arme = armer_panne()
    donnees = extract()
    arme >> donnees
    load(transform(validate(donnees)))


demo_panne()