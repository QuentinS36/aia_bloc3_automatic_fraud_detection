"""DAG de démonstration : injecte une vraie fraude du dataset dans le pipeline.

Usage : dans l'interface Airflow, ouvrir ce DAG puis cliquer sur "Trigger DAG".
Sert à valider la chaîne complète (et la notification Discord) sans attendre
qu'une fraude apparaisse naturellement dans l'API (environ 1 tous les 6 h).
"""
import os
import sys
from datetime import datetime

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/src")
import demo
import pipeline  

DATA_PATH = os.environ.get("DEMO_DATA_PATH", "/opt/airflow/data/fraudTest.csv")


@dag(
    dag_id="demo_inject_fraud",
    description="Injecte une fraude connue du dataset dans le pipeline (démonstration)",
    schedule=None,          # jamais automatique : uniquement au clic sur "Trigger DAG"
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo"],
)
def demo_inject_fraud():

    @task
    def inject_and_process():
        row = demo.pick_fraud_transaction(DATA_PATH)
        result = pipeline.process(row)
        print("Transaction injectée :", row["trans_num"], "| index d'origine :", row["_orig_index"])
        print("Résultat :", {k: v for k, v in result.items() if k != "prediction"})
        if result.get("prediction"):
            print("Prédiction :", result["prediction"])
        return result

    inject_and_process()


demo_inject_fraud()
