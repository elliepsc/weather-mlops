#!/usr/bin/env bash
# Lancement Airflow 3 standalone pour ce projet
# Usage: bash start_airflow.sh  OU  bash ~/start_airflow.sh
#
# UI disponible par défaut sur http://localhost:8083
# Login : admin / voir ~/airflow/simple_auth_manager_passwords.json.generated
# Arrêt  : Ctrl+C

set -euo pipefail

is_repo_dir() {
    local dir="$1"

    [[ -d "$dir/dags" \
        && -d "$dir/pipeline" \
        && -f "$dir/config/settings.py" ]]
}

real_dir() {
    cd "$1" && pwd -P
}

find_repo_dir() {
    local script_dir current_dir candidate git_root

    script_dir="$(real_dir "$(dirname "${BASH_SOURCE[0]}")")"
    current_dir="$(pwd -P)"

    for candidate in \
        "${WEATHER_RAIN_REPO_DIR:-}" \
        "$script_dir" \
        "$current_dir" \
        "$HOME/weather-rain"
    do
        [[ -n "$candidate" && -d "$candidate" ]] || continue

        git_root="$(git -C "$candidate" rev-parse --show-toplevel 2>/dev/null || true)"

        if [[ -n "$git_root" && -d "$git_root" ]] && is_repo_dir "$git_root"; then
            real_dir "$git_root"
            return 0
        fi

        if is_repo_dir "$candidate"; then
            real_dir "$candidate"
            return 0
        fi
    done

    return 1
}

REPO_DIR="$(find_repo_dir)" || {
    echo "Impossible de trouver le repo weather-rain." >&2
    echo "Lance ce script depuis le repo ou définis :" >&2
    echo "  export WEATHER_RAIN_REPO_DIR=/chemin/vers/weather-rain" >&2
    exit 1
}

# ── .env ──────────────────────────────────────────────────────────────────────
ENV_FILE="${WEATHER_RAIN_ENV_FILE:-$REPO_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

AIRFLOW_PORT="${AIRFLOW_PORT:-8083}"

# ── Symlink sans espaces ──────────────────────────────────────────────────────
# Airflow peut mal gérer les chemins Windows avec espaces.
# On utilise donc ~/weather-rain comme chemin propre pour Airflow.
REPO_LINK="${WEATHER_RAIN_REPO_LINK:-$HOME/weather-rain}"

if [[ "$REPO_LINK" != "$REPO_DIR" ]]; then
    if [[ -e "$REPO_LINK" && ! -L "$REPO_LINK" ]]; then
        echo "$REPO_LINK existe déjà et n'est pas un symlink." >&2
        echo "Supprime-le ou définis WEATHER_RAIN_REPO_LINK vers un autre chemin." >&2
        exit 1
    fi

    ln -sfn "$REPO_DIR" "$REPO_LINK"
fi

AIRFLOW_PROJECT_DIR="$REPO_LINK"

echo "Repo détecté    : $REPO_DIR"
echo "Chemin Airflow : $AIRFLOW_PROJECT_DIR"
echo "Port Airflow   : $AIRFLOW_PORT"

# ── Port ──────────────────────────────────────────────────────────────────────
export AIRFLOW__API__PORT="$AIRFLOW_PORT"
export AIRFLOW__API__BASE_URL="http://localhost:$AIRFLOW_PORT"
export AIRFLOW__WEBSERVER__BASE_URL="http://localhost:$AIRFLOW_PORT"

# ── DAGs + PYTHONPATH ─────────────────────────────────────────────────────────
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_PROJECT_DIR/dags"
export PYTHONPATH="$AIRFLOW_PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"

# ── Divers ────────────────────────────────────────────────────────────────────
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=60

cd "$HOME"
airflow standalone
