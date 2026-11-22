import sys
from datetime import datetime, timedelta

from airflow.sdk import CronDataIntervalTimetable, Param, dag, task
from airflow.sdk.exceptions import AirflowFailException

sys.path.append("/opt/airflow/src")
import cyber_pipeline as cp


@dag(
    dag_id="pipeline_cybersecurite",
    # Une execution par mois de donnees (planification automatique + backfill)
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=datetime(2020, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Pour un declenchement manuel : indiquer le mois a traiter (ex. 2022-03)
    params={
        "mois": Param(
            "",
            type="string",
            description="Mois a traiter (AAAA-MM). Laisser vide pour la periode planifiee.",
        )
    },
    default_args={
        "owner": "noura",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=5),
    },
)
def pipeline_cybersecurite():

    @task
    def extract(params=None, data_interval_start=None):
        try:
            mois = cp.choisir_mois(params, data_interval_start)
        except ValueError as erreur:
            # Erreur de saisie : inutile de reessayer, on echoue tout de suite
            raise AirflowFailException(str(erreur))
        return cp.extract(mois)

    @task
    def validate(info):
        return cp.validate(info)

    @task
    def transform(info):
        return cp.transform(info)

    @task
    def load(info):
        return cp.load(info)

    load(transform(validate(extract())))


pipeline_cybersecurite()