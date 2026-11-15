"""
DAG du rapport quotidien : statistiques de la veille, envoyées chaque matin.

Attention : le déclenchement est en UTC (Airflow travaille toujours en
UTC en interne). "0 6 * * *" correspond à 8h en France l'été (UTC+2) et 7h
l'hiver (UTC+1). 
"""
import sys
from datetime import datetime

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/src")
import daily_report  # noqa: E402


@dag(
    dag_id="daily_report",
    description="Rapport quotidien : transactions et fraudes de la veille",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting"],
)
def daily_report_dag():

    @task
    def send_report():
        stats = daily_report.run()
        print("Statistiques envoyées :", stats)
        return stats

    send_report()


daily_report_dag()
