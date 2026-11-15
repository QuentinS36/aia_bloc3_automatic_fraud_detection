"""DAG de chargement par lots : S3/MinIO -> Neon (ELT).

Toutes les 15 minutes plutôt qu'à chaque minute (cf. ingestion) : Neon peut
ainsi se remettre en veille entre deux chargements, ce qui reste dans le
quota de calcul gratuit (cf. l'estimation faite lors du cadrage).
"""
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/src")
import neon_loader  # noqa: E402


@dag(
    dag_id="load_neon",
    description="Charge par lots les zones raw/predictions/quarantine de S3 vers Neon",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["neon", "elt"],
)
def load_neon():

    @task
    def load(**context):
        counts = neon_loader.run(dag_id="load_neon", run_id=context["run_id"])
        print("Lignes chargées :", counts)
        return counts

    load()


load_neon()
