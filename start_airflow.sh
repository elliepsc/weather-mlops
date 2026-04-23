#!/bin/bash
# Lancement Airflow standalone pour ce projet
# Usage: bash start_airflow.sh
#
# UI disponible sur localhost:8083
# Login : admin / voir ~/airflow/simple_auth_manager_passwords.json.generated
# Arrêt  : Ctrl+C

# Chemin absolu vers la racine du repo (indépendant du répertoire courant)
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# Port de l'UI Airflow — 8083 car 8080/8081/8082 sont occupés sur cette machine
export AIRFLOW__API__PORT=8083

# Pointe Airflow vers les DAGs du projet au lieu du dossier par défaut ~/airflow/dags/
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_DIR/dags"

# Désactive les 90+ DAGs exemples fournis par Airflow (tutorials, demos)
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Désactive les connexions exemples (S3, GCP, etc.) inutiles ici
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False

# Lance depuis ~ pour éviter les erreurs os.getcwd() liées aux espaces dans le chemin WSL/NTFS
cd ~ && airflow standalone
