import sys
import time
from datetime import datetime

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")
import contrats as ct
import cyber_pipeline as cp

MOIS = "2022-03"


@dag(
    dag_id="demo_contrat",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "casser": Param(False, type="boolean",
                        description="Coche pour que validate transmette un resultat incomplet"),
    },
    default_args={"owner": "noura", "retries": 0},
)
def demo_contrat():

    @task
    def extract():
        debut = time.time()
        info = cp.extract(MOIS)
        ct.resume("extract", debut, lignes=info["nb_lignes"])
        return info

    @task
    def validate(info, params=None):
        debut = time.time()
        res = cp.validate(info)
        if params["casser"]:
            res.pop("path")  # simule un bug : validate oublie de transmettre le fichier
        ct.resume("validate", debut, valides=res["valides"], rejetees=res["rejetees"])
        return res

    @task
    def transform(info):
        debut = time.time()
        ct.verifier_contrat(info, ct.CONTRAT_VALIDATE_VERS_TRANSFORM, "validate -> transform")
        res = cp.transform(info)
        ct.resume("transform", debut, lignes=res["nb_lignes"], colonnes=res["nb_colonnes"])
        return res

    @task
    def load(info):
        debut = time.time()
        ct.verifier_contrat(info, ct.CONTRAT_TRANSFORM_VERS_LOAD, "transform -> load")
        res = cp.load(info)
        ct.resume("load", debut, mois_ecrits=len(res), lignes=sum(res.values()))
        return res

    load(transform(validate(extract())))


demo_contrat()