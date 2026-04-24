#!/bin/bash
# Lancement Airflow 3 standalone pour ce projet
# Usage: bash start_airflow.sh  OU  bash ~/start_airflow.sh
#
# UI disponible sur http://localhost:8083
# Login : admin / voir ~/airflow/simple_auth_manager_passwords.json.generated
# Arrêt  : Ctrl+C

# Chemin du repo — fixe, indépendant de l'endroit où le script est lancé
REPO_DIR="/mnt/c/Users/Ellie Pro/Documents/Projets Data/projets_github/weather-rain"

# ── Symlink sans espaces ──────────────────────────────────────────────────────
# Airflow 3 LocalExecutor spawn des subprocessus via subprocess.Popen.
# Les espaces dans le chemin Windows cassent ces commandes.
# ~/weather-rain est un symlink sans espaces vers le repo.
REPO_LINK="$HOME/weather-rain"
ln -sfn "$REPO_DIR" "$REPO_LINK"

# ── Port ──────────────────────────────────────────────────────────────────────
# BASE_URL doit correspondre au port — sinon les appels API internes
# (trigger, XCom, logs) partent sur localhost:8080 et échouent.
export AIRFLOW__API__PORT=8083
export AIRFLOW__API__BASE_URL="http://localhost:8083"
export AIRFLOW__WEBSERVER__BASE_URL="http://localhost:8083"

# ── DAGs + PYTHONPATH ─────────────────────────────────────────────────────────
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_LINK/dags"
export PYTHONPATH="$REPO_LINK"

# ── Divers ────────────────────────────────────────────────────────────────────
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=60

cd ~ && airflow standalone
