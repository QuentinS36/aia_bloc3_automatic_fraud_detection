"""DAG d'ingestion : appelle l'API toutes les minutes, valide, stocke.

"""
import sys
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

sys.path.insert(0, "/opt/airflow/src")
import ingestion as ing  
import notify  
import scoring 


default_args = {
    "owner": "fraud-detection",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="ingest_transactions",
    description="Ingestion de l'API de paiements (1 appel/minute) vers la zone brute",
    schedule="* * * * *",       # toutes les minutes, cf. exploration de l'API
    start_date=datetime(2026, 1, 1),
    catchup=False,               # on ne rejoue jamais les créneaux passés
    max_active_runs=1,           # une exécution à la fois : jamais deux appels API en parallèle
    default_args=default_args,
    tags=["ingestion"],
)
def ingest_transactions():

    @task
    def fetch():
        try:
            return ing.fetch_transaction()
        except Exception as e:
            # On ne fait pas planter le DAG pour une panne réseau ponctuelle :
            # le prochain créneau (dans 1 minute) réessaiera de lui-même.
            raise AirflowSkipException(f"API indisponible, on réessaiera dans 1 minute : {e}")

    @task
    def check_and_store(row: dict):
        valid, errors = ing.validate(row)
        key = ing.store(row, valid, errors)
        if not valid:
            print(f"Transaction en quarantaine ({key}) : {errors}")
        return {"key": key, "valid": valid, "row": row}

    @task
    def score_and_notify(checked: dict):
        if not checked["valid"]:
            print("Transaction en quarantaine : pas de scoring (données non fiables).")
            return None

        row = checked["row"]
        prediction = scoring.score(row)
        prediction["seen_in_training"] = ing.seen_in_training(row["_orig_index"])

        day = ing.ingestion_day(row)
        ing.s3_client().put_object(
            Bucket=ing.BUCKET,
            Key=f"predictions/dt={day}/{prediction['trans_num']}.json",
            Body=json.dumps(prediction, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        if prediction["is_fraud_predicted"]:
            notify.notify_fraud(prediction)

        return prediction

    score_and_notify(check_and_store(fetch()))


ingest_transactions()
