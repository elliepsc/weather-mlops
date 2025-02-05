from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import subprocess
import os
import json
from tools import *

# Variables globales
DRIFT_SCRIPT_PATH = "/opt/airflow/dags/monitoring/drift_monitoring.py"
MODEL_COMPARISON_SCRIPT_PATH = "/opt/airflow/dags/monitoring/model_comparison.py"
DRIFT_SUMMARY_PATH = "/opt/airflow/data/monitoring/drift_summary.json"

# Tâche 1 : Exécuter la détection de dérive
def run_drift_monitoring():
    # Exécute le script de détection de dérive
    subprocess.run(["python", DRIFT_SCRIPT_PATH], check=True)

# Tâche 2 : Comparer les modèles
def run_model_comparison():
    # Exécute le script de comparaison des modèles
    subprocess.run(["python", MODEL_COMPARISON_SCRIPT_PATH], check=True)

# Tâche 3 : Vérifier si une dérive est détectée et générer une alerte
def check_drift_and_alert():
    # Vérifie que le fichier JSON existe
    if not os.path.exists(DRIFT_SUMMARY_PATH):
        raise FileNotFoundError(f"Le fichier {DRIFT_SUMMARY_PATH} est introuvable.")
    
    # Ouvre et lit le fichier JSON
    with open(DRIFT_SUMMARY_PATH, "r") as f:
        drift_summary = json.load(f)
    
    # On considère qu'une dérive est détectée si au moins une feature a "drift_detected" à True
    drift_detected = any(feature_info.get("drift_detected", False) for feature_info in drift_summary.values())
    
    if drift_detected:
        message = "Dérive détectée dans les données de production !"
        print(message)
        return "send_email_alert"
    else:
        return "end_dag"# Si aucune dérive n'est détectée, la tâche se termine normalement.

def email_alert():
    print("An email will be sent")

# Définition des paramètres du DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="monitoring_dag",
    default_args=default_args,
    description="DAG pour le monitoring de dérive et comparaison des modèles",
    schedule_interval="0 0 * * *",  # Planification quotidienne à minuit
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Étape 1 : Détection de dérive
    drift_monitoring_task = PythonOperator(
        task_id="run_drift_monitoring",
        #python_callable=run_drift_monitoring,
        python_callable=start_existing_container,
        op_kwargs={'container_name': 'meteo_group-monitoring-1'}
    )

    # Étape 2 : Comparaison des modèles
    """
    model_comparison_task = PythonOperator(
        task_id="run_model_comparison",
        python_callable=run_model_comparison,
    )
    """
    # Étape 3 : Vérification et déclenchement d'une alerte en cas de dérive
    check_drift_task = BranchPythonOperator(
        task_id="check_drift_and_alert",
        python_callable=check_drift_and_alert,
    )

    # Étape 4 : Envoi d'un email en cas de dérive détectée
    email_alert_task = PythonOperator(
        task_id="send_email_alert",
        python_callable=email_alert,
        #to="team@example.com",
        #subject="Alerte : Dérive détectée dans les données !",
        #html_content="Des dérives ont été détectées dans les données de production. Veuillez vérifier le rapport.",
    )
     #Dummy task
    end_dag=DummyOperator(task_id="end_dag")

    # Orchestration des tâches :
    # D'abord, exécuter la détection, puis la vérification.
    # En cas d'erreur dans la vérification (dérive détectée), Airflow considèrera la tâche comme échouée et pourra déclencher l'alerte.
    drift_monitoring_task >> check_drift_task >> [email_alert_task, end_dag]
