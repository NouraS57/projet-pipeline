import csv
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

log = logging.getLogger(__name__)
CSV_PATH = "/opt/airflow/data/raw/cybersecurity_attacks.csv"


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
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            lecteur = csv.reader(f)
            colonnes = next(lecteur)
            nb_lignes = sum(1 for _ in lecteur)
        log.info("extract : %s colonnes, %s lignes lues", len(colonnes), nb_lignes)
        return {"colonnes": colonnes, "nb_lignes": nb_lignes}

    @task
    def validate(infos):
        if len(infos["colonnes"]) != 25:
            raise ValueError(f"25 colonnes attendues, {len(infos['colonnes'])} recues")
        log.info("validate : schema OK (%s lignes)", infos["nb_lignes"])
        return infos["nb_lignes"]

    @task
    def load(nb_lignes):
        log.info("load : %s lignes pretes a etre chargees (a completer au TP)", nb_lignes)

    load(validate(extract()))


pipeline_cybersecurite()