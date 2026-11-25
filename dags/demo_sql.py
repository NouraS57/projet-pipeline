import sys
from datetime import datetime

from airflow.sdk import dag, task

sys.path.append("/opt/airflow/src")
import sql_ingestion as sql

MOIS = "2022-03"


@dag(
    dag_id="demo_sql",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "noura", "retries": 0},
)
def demo_sql():

    @task
    def preparer_base():
        return sql.preparer_referentiel()

    @task
    def lire_et_enrichir(info):
        return sql.enrichir_par_severite(info, MOIS)

    lire_et_enrichir(preparer_base())


demo_sql()