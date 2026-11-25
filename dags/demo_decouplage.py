import sys
from datetime import datetime

from airflow.sdk import Param, dag, task

sys.path.append("/opt/airflow/src")
import cyber_pipeline as cp
import decouplage as dc

MOIS = "2022-03"


@dag(
    dag_id="demo_decouplage",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    params={
        "casser_plat": Param(False, type="boolean",
                             description="Coche pour simuler une panne de la branche fichier plat"),
    },
    default_args={"owner": "noura", "retries": 0},
)
def demo_decouplage():

    # --- Branche 1 : fichier CSV ---
    @task
    def extract_csv():
        return cp.extract(MOIS)

    @task
    def validate_csv(info):
        return cp.validate(info)

    # --- Branche 2 : fichier plat ---
    @task
    def deposer_fichier_plat():
        return dc.generer_fichier_plat(MOIS)

    @task
    def lire_fichier_plat(chemin, params=None):
        if params["casser_plat"]:
            raise RuntimeError("PANNE SIMULEE dans la branche fichier plat")
        return dc.lire_fichier_plat(chemin, MOIS)

    # --- Fusion des deux branches ---
    @task
    def fusionner(info_csv, info_plat):
        return dc.fusionner(info_csv, info_plat)

    fusionner(validate_csv(extract_csv()), lire_fichier_plat(deposer_fichier_plat()))


demo_decouplage()