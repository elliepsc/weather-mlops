# RUNBOOK — Reproductibilité complète du projet

> Ce fichier est le guide opérationnel de A à Z.
> Il couvre l'installation, le premier lancement, l'exploitation quotidienne, l'analyse des modèles et les procédures de récupération.

---

## Réponse directe : peut-on tout faire via l'orchestrateur Airflow ?

**Non — pas entièrement.** Voici la répartition :

| Action | CLI manuel | Airflow DAG |
|---|:---:|:---:|
| Installation Python / venv / pip | ✅ obligatoire | ✗ |
| Copie `.env` | ✅ obligatoire | ✗ |
| Backfill initial (2008 → J-1) | ✅ `run_pipeline.py backfill` | ✅ `weather_backfill` (déclencher manuellement) |
| Mise à jour quotidienne | ✅ `run_pipeline.py daily` | ✅ `weather_daily_ingestion` (automatique 6h UTC) |
| Réentraînement hebdomadaire | ✅ `run_pipeline.py train` | ✅ `weather_weekly_train` (automatique lundi 2h) |
| Monitoring qualité / drift | ✗ | ✅ `weather_daily_monitoring` (automatique 9h UTC) |
| Gap monitoring | ✗ | ✅ `weather_gap_monitor` (automatique) |
| Export CSV | ✅ `run_pipeline.py export` | ✅ inclus dans chaque DAG |
| Démarrer Airflow lui-même | ✅ obligatoire | ✗ |
| Démarrer l'API FastAPI | ✅ obligatoire | ✗ |
| Démarrer Streamlit | ✅ obligatoire | ✗ |
| Démarrer Docker (Prometheus/Grafana) | ✅ obligatoire | ✗ |
| Démarrer MLflow UI | ✅ obligatoire | ✗ |

**Conclusion :** L'orchestrateur gère tout le cycle de données (ingestion → entraînement → prédiction → monitoring).
L'infrastructure (services, environnement) doit être démarrée manuellement ou via un gestionnaire de processus (supervisord, systemd, screen).

---

## PARTIE 1 — Installation (une seule fois)

### 1.1 Cloner le projet

```bash
git clone https://github.com/elliepsc/meteo.git weather-mlops
cd weather-mlops
```

### 1.2 Environnement Python

```bash
python -m venv .venv

# Activer l'environnement
source .venv/bin/activate          # Linux / macOS / WSL
.venv\Scripts\Activate.ps1         # Windows PowerShell
```

### 1.3 Dépendances

```bash
pip install -r requirements.txt
```

Versions clés installées :

| Package | Version | Rôle |
|---|---|---|
| xgboost | 3.x | Modèles ML |
| mlflow | 3.x | Tracking expériences |
| fastapi | 0.13x | API REST |
| streamlit | 1.4x | Dashboard |
| requests | 2.x | Appels Open-Meteo |
| pandas | 2.x | Manipulation données |

### 1.4 Configuration environnement

```bash
cp .env.example .env
```

Variables importantes dans `.env` :

```env
# Laisser commenté pour le fallback automatique :
# - Windows / Docker : <repo>/mlflow/mlflow.db
# - WSL + repo sous /mnt/... : ~/.weather-mlops/mlflow/mlflow.db
# MLFLOW_TRACKING_URI=sqlite:///mlflow/mlflow.db
API_HOST=0.0.0.0
API_PORT=8001
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=weather
```

> Open-Meteo est gratuit et sans clé API. Aucune variable secrète requise pour le fonctionnement de base.
> Pour les alertes Slack, renseigner `SLACK_WEBHOOK_URL`.

---

## PARTIE 2 — Premier lancement (backfill)

### 2.1 Via CLI (recommandé pour la première fois)

```bash
python pipeline/run_pipeline.py backfill
```

Étapes exécutées dans l'ordre :
1. `init_db` — crée `data/weather.db` + tables + vue `v_weather_full`
2. Télécharge 2008-01-01 → J-1 pour les 26 villes (Open-Meteo, 10 s entre villes)
3. Répare les éventuels trous (`repair_gaps`)
4. Entraîne les 6 modèles XGBoost (~10 min sur 173 k lignes)
5. Génère ~173 k prédictions
6. Exporte `data/output/weather_final.csv`

**Durée totale : ~15 minutes**

> **Rate limit Open-Meteo :** si certaines villes échouent (erreur 429), relancer la même commande.
> Les villes déjà à jour sont ignorées automatiquement. Seules les villes manquantes sont re-téléchargées.

### 2.2 Via Airflow DAG (alternative)

```
DAG : weather_backfill
Trigger : manuel → "Trigger DAG w/ config"
Config JSON :
{
  "start_date": "2008-01-01",
  "end_date":   "2026-04-27"
}
```

> Le DAG backfill et le CLI utilisent tous les deux `delay_seconds=10` entre villes. Comportement identique.

### 2.3 Vérification post-backfill

```bash
python -c "
import sqlite3, json
conn = sqlite3.connect('data/weather.db')
r = conn.execute('SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT city) FROM weather_raw').fetchone()
print(f'Lignes brutes : {r[0]:,}')
print(f'Période       : {r[1]} -> {r[2]}')
print(f'Villes        : {r[3]}/26')
p = conn.execute('SELECT COUNT(*) FROM weather_predictions').fetchone()[0]
print(f'Prédictions   : {p:,}')
conn.close()

with open('models/metrics.json') as f:
    m = json.load(f)
print()
print('Métriques modèles :')
for k, v in m.items():
    print(f'  {k} :', v)
"
```

Résultats attendus :
- Lignes brutes : ~173 800
- Villes : 26/26
- `rain_tomorrow` AUC > 0.85
- `max_temp_tomorrow` R² > 0.90

---

## PARTIE 3 — Analytics (dbt + DuckDB)

La couche analytics transforme les données SQLite brutes en tables Power BI via dbt + DuckDB.
Voir `ANALYTICS.md` pour l'architecture complète.

### 3.1 Installation (première fois)

```bash
pip install dbt-core==1.11.8 dbt-duckdb==1.10.1 duckdb==1.5.2
# ou via Make :
make analytics-install
```

**Outils externes (GUI, installation séparée) :**
- **DBeaver** (SQL client) : https://dbeaver.io — connection native DuckDB depuis v23
- **DuckDB ODBC driver** (Power BI live) : https://duckdb.org/docs/api/odbc/overview

### 3.2 Pipeline analytics complet

```bash
# Étape 1 — charger SQLite + fichiers JSON monitoring → DuckDB
python analytics/scripts/load_sources.py

# Étape 2 — installer les packages dbt (dbt_utils) et construire les modèles
cd analytics && dbt deps && dbt run

# Étape 3 — exporter les marts en CSV pour Power BI
python analytics/scripts/export_powerbi.py

# Tout en une commande via Make :
make analytics-all
```

### 3.3 Explorer les tables dans DuckDB

**CLI DuckDB :**
```bash
duckdb data/analytics.duckdb
```
```sql
SHOW SCHEMAS;
-- main_staging, main_core, main_intermediate, main_marts

SELECT * FROM main_marts.mart_mlops_health LIMIT 10;
SELECT * FROM main_marts.mart_model_performance_by_city ORDER BY month DESC;
SELECT * FROM main_marts.mart_forecast_vs_actual_timeline WHERE city = 'Sydney' LIMIT 20;
```

**DBeaver (GUI) :**
1. New Connection → **DuckDB** (support natif v23+)
2. Path : `<repo>/data/analytics.duckdb`
3. Schémas disponibles après `dbt run` :
   - `main_staging` — vues de staging (typage 1:1)
   - `main_core` — `dim_cities`
   - `main_intermediate` — jointures et calculs partagés
   - `main_marts` — tables finales Power BI

### 3.4 Connexion Power BI

**Option A — Import CSV (le plus simple) :**
```bash
make analytics-export
# → génère data/analytics/*.csv (un fichier par mart)
```
Dans Power BI Desktop : **Obtenir les données → Texte/CSV** → sélectionner le fichier.

Tables disponibles :
| Fichier CSV | Grain | Contenu |
|---|---|---|
| `mart_model_performance_overview.csv` | mois | Accuracy + MAE globaux |
| `mart_model_performance_by_city.csv` | ville × mois | Dégradation par ville |
| `mart_forecast_vs_actual_timeline.csv` | ville × jour (90j) | Prédictions vs réel |
| `mart_mlops_health.csv` | jour (30j) | Complétude + décisions ops |
| `mart_retraining_history.csv` | événement retrain | Audit avant/après retrain |

**Option B — Connexion live via ODBC :**
1. Télécharger et installer le DuckDB ODBC driver (lien ci-dessus)
2. Créer un DSN système pointant sur `data/analytics.duckdb`
3. Power BI Desktop : **Obtenir les données → ODBC** → sélectionner le DSN
4. Mode Import (DirectQuery DuckDB local non recommandé)
5. Tables dans le schéma `main_marts`

**Option C — Via URL raw GitHub (recommandé Power BI Desktop distant) :**

Le DAG `weather_dbt_analytics` pousse automatiquement les `mart_*.csv` sur GitHub
après chaque export. Power BI Desktop peut les lire directement sans dépendance locale.

URLs brutes (à coller dans Power BI → Obtenir les données → Web) :
```
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_overview.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_by_city.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_forecast_vs_actual_timeline.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_mlops_health.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_retraining_history.csv
```

Voir `analytics/data_viz/template.pq` pour les requêtes Power Query M prêtes à l'emploi
et `analytics/data_viz/SOURCES.md` pour le schéma de chaque mart.

**Prérequis — variable `GH_TOKEN` :**

Le push automatique nécessite un Personal Access Token GitHub avec permission
`Contents: read & write` sur ce dépôt.

1. Créer le token : [github.com/settings/tokens](https://github.com/settings/tokens)
   → "Fine-grained tokens" → Repository : `weather-mlops` → Contents : Read & Write
2. Copier le token (visible une seule fois)
3. L'ajouter au `.env` local :
   ```env
   GH_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
   ```
4. L'ajouter comme variable Airflow (pour que le DAG y accède en production) :
   Airflow UI → **Admin → Variables → +** → Key : `GH_TOKEN`, Value : `ghp_xxx...`

> Sans `GH_TOKEN`, la tâche `git_push_analytics` lève une `RuntimeError` et le DAG échoue.
> L'export CSV local (`t_export`) réussit toujours avant cette tâche.

**Option D — Via l'API FastAPI (données brutes, sans agrégations dbt) :**
```text
http://localhost:8001/api/weather        → toutes les données
http://localhost:8001/api/export/csv     → weather_final.csv
```

### 3.5 Exploration interactive — notebooks Jupyter

```bash
pip install jupyter matplotlib seaborn
jupyter notebook notebooks/01_exploration.ipynb
```

Le notebook `notebooks/01_exploration.ipynb` couvre :
- Audit qualité (NULLs, volumétrie, plages de dates)
- Tendances de performance modèle + saisonnalité
- Classement des villes par accuracy/MAE
- Distribution des erreurs de prédiction
- MLOps health (complétude, décisions, retrains)

### 3.6 Schéma de régénération

À relancer après chaque ingestion quotidienne pour mettre à jour les marts Power BI :
```bash
make analytics-all
# = load_sources.py + dbt run + export_powerbi.py (~30 secondes)
```

---

## PARTIE 4 — Démarrer les services

### 3.1 API FastAPI (obligatoire pour Streamlit et Power BI)

```bash
python api/app.py
# → http://localhost:8001
# → http://localhost:8001/docs  (Swagger)
```

Vérification :
```bash
curl http://localhost:8001/health
# Attendu : {"status":"ok","db":true}

curl "http://localhost:8001/api/weather/latest?city=Sydney"
```

### 3.2 Dashboard Streamlit

```bash
# Terminal séparé (API doit tourner)
streamlit run streamlit_app/app.py
# → http://localhost:8501
```

### 3.3 MLflow UI

```bash
# WSL (repo sous /mnt/...)
mlflow ui --backend-store-uri sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db --port 5000

# Windows natif ou Docker
mlflow ui --backend-store-uri sqlite:///mlflow/mlflow.db --port 5000
# → http://localhost:5000  (expérience : weather_australia)
```

### 3.4 Docker Compose — tous les services (API + Prometheus + Grafana + Airflow)

#### Premier lancement

```bash
docker compose up -d
```

`-d` = détaché, sinon le terminal est bloqué indéfiniment.
Ne pas utiliser `--build` sauf si les Dockerfiles ont changé.

| Service | URL | Identifiants |
|---|---|---|
| API FastAPI | http://localhost:8000 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Airflow UI | http://localhost:8083 | voir ci-dessous |

> **Note port API :** en local (sans Docker), l'API tourne sur **8001** (variable `API_PORT` dans `.env`).
> Dans Docker, `docker-compose.yaml` force le port **8000** et mappe `8000:8000`.

#### Identifiants Airflow (SimpleAuthManager — Airflow 3.x)

Au **premier démarrage**, Airflow génère un mot de passe aléatoire et l'affiche dans les logs :

```bash
docker compose logs airflow-webserver | grep "Password for user"
# Simple auth manager | Password for user 'admin': <mot_de_passe_généré>
```

Pour fixer un mot de passe permanent via le fichier de mots de passe :

```bash
# Modifier config/simple_auth_manager_passwords.json
# Format : {"users": [{"username": "admin", "password": "mon_mdp"}]}
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

#### Après une modification du `docker-compose.yaml`

Un simple `docker compose up -d` **ne recrée pas** les containers existants.
Si tu changes des volumes ou des variables d'environnement :

```bash
# Recréer uniquement les services modifiés
docker compose up -d --force-recreate airflow-scheduler airflow-webserver

# Ou tout recréer (plus sûr)
docker compose up -d --force-recreate
```

#### Compatibilité Airflow 3.x (breaking changes vs 2.x)

| Ancienne commande (Airflow 2.x) | Nouvelle commande (Airflow 3.x) |
|---|---|
| `command: webserver` | `command: api-server` |
| `airflow webserver` | `airflow api-server` |
| `airflow db init` | `airflow db migrate` |

Volumes **obligatoires** dans les 3 services Airflow (`airflow-init`, `airflow-webserver`, `airflow-scheduler`) :

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./pipeline:/opt/airflow/pipeline
  - ./config:/opt/airflow/config   # ← requis : les DAGs importent config.settings
  - ./data:/opt/airflow/data
  - ./models:/opt/airflow/models
  - ./mlflow:/opt/airflow/mlflow
  - airflow_logs:/opt/airflow/logs
```

Sans `./config`, tous les DAGs échouent avec `ModuleNotFoundError: No module named 'config'`.

#### Vérifier que les DAGs sont chargés

```bash
docker compose exec airflow-scheduler airflow dags list
# Attendu : 5 DAGs listés

# Si vide ou erreur :
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

## PARTIE 4 — Airflow (orchestration automatique)

### 4.1 Installation Airflow (mode standalone local / WSL)

```bash
pip install apache-airflow
```

### 4.2 Initialisation (une seule fois — Airflow 3.x)

```bash
# Migrer / initialiser la DB Airflow
airflow db migrate

# Créer l'utilisateur admin
airflow users create \
  --username admin \
  --password weather \
  --role Admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@example.com
```

### 4.3 Démarrer Airflow

**Option A — Standalone tout-en-un (mode dev)**

```bash
bash start_airflow.sh
# UI : http://localhost:8083
```

**Option B — Webserver + scheduler séparés**

```bash
# Terminal 1
airflow api-server --port 8083   # port 8083 pour Airflow UI

# Terminal 2
airflow scheduler
```

UI : http://localhost:8083 — identifiants : `admin / weather`

### 4.3 bis — Symlink vers `start_airflow.sh` (depuis `~`)

```bash
# Arrêter Airflow
pkill -f "airflow standalone"

# Une seule fois, depuis le repo
ln -sfn "$(pwd)/start_airflow.sh" ~/start_airflow.sh

# Relancer
bash ~/start_airflow.sh
```

### 4.4 DAGs disponibles

| DAG | Schedule | Déclenchement | Tâches principales |
|---|---|---|---|
| `weather_daily_ingestion` | `0 6 * * *` | Automatique | init_db → fetch J-1 → check qualité → predict → export → **sync_to_postgres** |
| `weather_weekly_train` | `0 2 * * 1` | Automatique (lundi) | snapshot baseline → retrain → compare baseline → rollback si dégradé sinon predict → export |
| `weather_daily_monitoring` | `0 9 * * *` | Automatique | qualité → couverture → drift KS → métriques → décision 4 voies |
| `weather_backfill` | Manuel | Trigger UI avec config JSON | fetch historique → repair gaps → retrain → predict → export |
| `weather_gap_monitor` | Planifié | Automatique | détection et réparation des trous dans `weather_raw` |

> `sync_to_postgres` est la dernière tâche du DAG `weather_daily_ingestion`. Elle est silencieuse si `NEON_DATABASE_URL` n'est pas défini — aucun breaking change en local.

### 4.5 Activer les DAGs

Dans l'UI Airflow (http://localhost:8083) :
1. Cliquer sur le toggle à gauche de chaque DAG pour l'activer
2. Pour `weather_backfill` : cliquer **Trigger DAG ▶** → fournir le JSON de config

### 4.6 Vérifier l'exécution d'un DAG

```bash
# Via CLI
airflow dags list
airflow tasks list weather_daily_ingestion
airflow dags trigger weather_daily_ingestion   # déclencher manuellement

# Voir les logs d'une tâche
airflow tasks logs weather_daily_ingestion fetch_daily <date_execution>
```

---

## PARTIE 5 — Mise à jour quotidienne

### 5.1 Automatique (Airflow actif)

Rien à faire. Le DAG `weather_daily_ingestion` s'exécute chaque jour à 6h UTC :
- Télécharge les données de la veille pour les 26 villes
- Régénère les prédictions
- Met à jour le CSV

### 5.2 Manuelle (sans Airflow)

```bash
python pipeline/run_pipeline.py daily
```

Durée : ~30 secondes.

### 5.3 Vérifier que les données sont à jour

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/weather.db')
rows = conn.execute('''
    SELECT city, MAX(date) as latest
    FROM weather_raw
    GROUP BY city
    ORDER BY latest ASC
    LIMIT 5
''').fetchall()
print('Villes les moins récentes :')
for r in rows: print(f'  {r[0]:<15} {r[1]}')
conn.close()
"
```

---

## PARTIE 6 — Réentraînement hebdomadaire

### 6.1 Automatique (Airflow actif)

Le DAG `weather_weekly_train` s'exécute chaque lundi à 2h UTC.

Il effectue une **validation baseline** avant mise en production :
1. Snapshot des modèles actuels dans `models/baseline/`
2. Réentraîne tous les modèles
3. Compare les métriques : si `rain_accuracy` chute de > 2 pp **ou** `temp_mae` augmente de > 0.2°C → rollback automatique vers `models/baseline/`
4. Si les métriques sont acceptables : génère les prédictions et exporte

### 6.2 Manuel

```bash
python pipeline/run_pipeline.py train
```

Durée : ~10 minutes sur 173 k lignes.

### 6.3 Comparer les métriques avant/après

```bash
python -c "
import mlflow, os
uri = os.getenv('MLFLOW_TRACKING_URI', f'sqlite:////home/{os.getenv(\"USER\", \"user\")}/.weather-mlops/mlflow/mlflow.db')
mlflow.set_tracking_uri(uri)
client = mlflow.tracking.MlflowClient()
exp = client.get_experiment_by_name('weather_australia')
runs = client.search_runs(exp.experiment_id, order_by=['start_time DESC'], max_results=3)
for r in runs:
    print(r.info.run_id[:8], r.info.start_time, {k: round(v,3) for k,v in r.data.metrics.items() if 'rain' in k or 'mae' in k})
"
```

---

## PARTIE 7 — Analyse XGBoost

### 7.1 Métriques actuelles

```bash
python -c "
import json
with open('models/metrics.json') as f:
    for name, metrics in json.load(f).items():
        print(f'{name}:')
        for k, v in metrics.items():
            print(f'  {k}: {round(v,4) if isinstance(v, float) else v}')
        print()
"
```

Métriques de référence (entraînement sur ~173 800 lignes, 2008-2026) :

| Modèle | Accuracy / MAE | R² | AUC | Seuil d'alerte |
|---|---|---|---|---|
| `rain_tomorrow` | 77.0% | — | 0.852 | AUC < 0.80 |
| `max_temp_tomorrow` | MAE 1.63°C | 0.907 | — | R² < 0.85 |
| `weather_type_tomorrow` | 82.2% | — | — | acc < 0.80 |
| `heatwave_risk` | — | — | 0.996 | AUC < 0.95 |
| `frost_risk` | — | — | 0.989 | AUC < 0.95 |
| `storm_probability` | — | — | 0.898 | AUC < 0.80 |

### 7.2 Feature importances

```bash
python -c "
import json, os
for f in sorted(os.listdir('models')):
    if 'feature_importance' in f:
        model = f.replace('_feature_importance.json', '')
        with open(f'models/{f}') as fh:
            top3 = list(json.load(fh).items())[:3]
        print(f'{model}:')
        for feat, score in top3:
            print(f'  {feat}: {score:.4f}')
        print()
"
```

### 7.3 Monitoring automatique — drift et accuracy

Le DAG `weather_daily_monitoring` (09:00 UTC) effectue chaque jour :

1. **Qualité des données** — vérifie que ≥ 80 % des 26 villes ont leurs données J-1
2. **Couverture des prédictions** — signale les villes dont les prédictions ne sont pas à jour
3. **Détection de drift** — test KS entre les 30 derniers jours et les 30 jours précédents sur 6 features (`max_temp`, `min_temp`, `humidity_3pm`, `pressure_3pm`, `rainfall`, `wind_gust_speed`)
4. **Métriques modèle** — accuracy pluie et MAE température sur 30 jours glissants
5. **Décision 4 voies** :

| Décision | Condition |
|---|---|
| `trigger_retrain` | Drift KS ≥ **4** features OU accuracy pluie < 75 % (cooldown respecté) |
| `alert_only` | Drift KS ≥ **3** features (probable saisonnalité, ne justifie pas un retrain) |
| `alert_insufficient_data` | < 30 lignes disponibles pour calculer les métriques |
| `no_action` | Modèle stable |

Le cooldown entre deux retrains automatiques est de **7 jours** (configurable dans `config/mlops.yaml` → `monitoring.retrain_cooldown_days`).

**Lire les résultats :**
```bash
cat data/monitoring/model_metrics.json   # accuracy + MAE
cat data/monitoring/drift_report.json    # KS stat par feature
cat data/monitoring/monitoring_decision.json  # décision retenue
```

**Lancement manuel :**
```bash
airflow dags trigger weather_daily_monitoring
```

**Dans l'UI :** tâche `trigger_retrain` verte = retrain lancé / `no_action` verte = modèle stable.

---

## PARTIE 8 — Power BI

### Option 1 — Connecteur Web (données en temps réel)

1. Ouvrir Power BI Desktop
2. **Obtenir les données > Web**
3. URL : `http://localhost:8001/api/weather`
4. Dans Power Query : développer la colonne `data`

Filtres disponibles via paramètres URL :
- `?city=Sydney`
- `?start_date=2024-01-01&end_date=2024-12-31`
- `?city=Melbourne&start_date=2025-01-01`

### Option 2 — Fichier CSV (statique)

```text
data/output/weather_final.csv
```

Mis à jour automatiquement par chaque run `daily`, `train` ou `export`.

---

## PARTIE 9 — Tests

```bash
python -m pytest tests/ -q
```

60 tests couvrant :
- Feature engineering (`test_preprocess.py`)
- Mapping enrichi Open-Meteo (`test_fetch_weather.py`)
- Entraînement et persistance modèles (`test_xgboost_model.py`)
- Configuration MLflow (`test_mlflow_config.py`)
- Configuration modèle YAML (`test_modeling_config.py`)
- Branching monitoring 4 voies (`test_monitoring_branch.py`)
- Gate baseline et rollback du train DAG (`test_train_dag.py`)
- Idempotence du daily ingestion et reprise incrémentale du backfill (`test_ingestion_backfill_flow.py`)

Résultat attendu : `60 passed`

---

## PARTIE 10 — Procédures de récupération

### 10.1 Villes manquantes (après rate limit Open-Meteo)

```bash
# Identifier les villes en retard
python -c "
import sqlite3
conn = sqlite3.connect('data/weather.db')
rows = conn.execute('''
    SELECT city, MAX(date) as latest, COUNT(*) as n
    FROM weather_raw
    GROUP BY city
    ORDER BY latest ASC
    LIMIT 10
''').fetchall()
for r in rows:
    print(f'  {r[0]:<15} {r[1]}  ({r[2]} lignes)')
conn.close()
"

# Re-télécharger uniquement les villes manquantes (sans --force)
python pipeline/run_pipeline.py backfill
```

### 10.2 Réparer un intervalle spécifique

```bash
python pipeline/run_pipeline.py repair --start-date 2026-01-01 --end-date 2026-04-27
```

### 10.3 Reconstruire la base de zéro

```bash
rm data/weather.db
python pipeline/run_pipeline.py backfill
```

### 10.4 Régénérer les prédictions sans réentraîner

```bash
python pipeline/run_pipeline.py predict
```

### 10.5 Régénérer le CSV uniquement

```bash
python pipeline/run_pipeline.py export
```

### 10.6 Forcer le re-téléchargement de toutes les villes

```bash
python pipeline/run_pipeline.py backfill --force
# Attention : efface et re-télécharge toutes les données (~15 min)
```

### 10.7 Rollback manuel des modèles

```bash
# Restaurer le baseline manuellement si le train DAG n'a pas rollbacké automatiquement
cp models/baseline/*.pkl models/
cp models/baseline/metrics.json models/
python pipeline/run_pipeline.py predict
```

### 10.8 Réinitialiser la DB Airflow (Docker)

```bash
docker compose down -v   # supprime aussi le volume PostgreSQL
docker compose up -d
# Les DAGs seront rechargés automatiquement ; les historiques d'exécution sont perdus
```

---

## PARTIE 11 — Déploiement cloud (Render + Neon + Streamlit Cloud)

### 11.1 Architecture cloud

```
Local (Airflow quotidien)
    SQLite (weather.db)
          │
          ▼ sync_to_postgres (06:xx UTC)
    Neon PostgreSQL (Frankfurt)
          │
          ▼
    Render API (weather-mlops-api.onrender.com)
          │
          ▼
    Streamlit Cloud (app Streamlit publique)
```

### 11.2 Setup initial Neon (une seule fois)

1. Créer un compte sur [neon.tech](https://neon.tech)
2. Créer un projet **weather-mlops** — région **AWS Frankfurt** (eu-central-1)
3. Copier la **Connection string** (onglet "Connection string", sans pooling) :
   ```
   postgresql://neondb_owner:xxxx@ep-xxx.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
4. Ajouter dans `.env` local :
   ```env
   NEON_DATABASE_URL=postgresql://neondb_owner:xxxx@ep-xxx.eu-central-1.aws.neon.tech/neondb?sslmode=require
   ```
5. Populer Neon pour la première fois (crée les tables + view + upsert 180 jours) :
   ```bash
   python -c "
   import logging; logging.basicConfig(level=logging.INFO)
   from pipeline.sync_to_postgres import sync_to_postgres
   sync_to_postgres()
   "
   ```
   Résultat attendu :
   ```
   INFO:pipeline.sync_to_postgres:Syncing 4675 raw rows, 2335 prediction rows...
   INFO:pipeline.sync_to_postgres:PostgreSQL sync complete.
   ```

### 11.3 Setup Render (une seule fois)

1. Dans le dashboard Render → service **weather-mlops-api** → **Environment**
2. Ajouter / vérifier ces variables :

| Clé | Valeur |
|---|---|
| `DEMO_MODE` | `false` |
| `NEON_DATABASE_URL` | `postgresql://neondb_owner:xxxx@ep-xxx...` |
| `PYTHON_VERSION` | `3.11.0` |

3. Redéployer manuellement ("Manual Deploy") ou attendre le push automatique

**Vérification :** ouvrir `https://weather-mlops-api.onrender.com/health`
Résultat attendu :
```json
{
  "status": "ok",
  "demo_mode": false,
  "db": "postgresql",
  "last_data_update": "2026-05-17"
}
```

### 11.4 Setup Streamlit Cloud (une seule fois)

1. Sur [streamlit.io/cloud](https://streamlit.io/cloud) → **New app** → dépôt `weather-mlops`
2. Dans **Settings → Secrets** :
   ```toml
   API_URL = "https://weather-mlops-api.onrender.com"
   ```
3. Aucune autre variable nécessaire — Streamlit lit uniquement l'API Render.

### 11.5 Cycle quotidien automatique

Chaque jour à 6h UTC, le DAG `weather_daily_ingestion` exécute :

```
init_db → fetch_daily → check_daily_ingestion → run_predictions → export_csv → sync_to_postgres
```

La dernière tâche `sync_to_postgres` pousse automatiquement les 180 derniers jours vers Neon.
Render lit Neon à chaque requête — pas de redéploiement nécessaire.

### 11.6 Sync manuel forcé

Pour resynchroniser immédiatement sans attendre le DAG :
```bash
python -c "
import logging; logging.basicConfig(level=logging.INFO)
from pipeline.sync_to_postgres import sync_to_postgres
sync_to_postgres()
"
```

Pour changer la fenêtre de rétention (ex. 365 jours raw, 180 jours prédictions) :
```bash
python -c "
from pipeline.sync_to_postgres import sync_to_postgres
sync_to_postgres(days_raw=365, days_predictions=180)
"
```

### 11.7 Dépannage

| Symptôme | Cause probable | Action |
|---|---|---|
| Render retourne `"db": "sqlite"` | `NEON_DATABASE_URL` absent ou vide dans Render env | Vérifier la variable dans Render → Environment |
| Render retourne `"demo_mode": true` | `DEMO_MODE=true` toujours actif | Passer `DEMO_MODE=false` dans Render env |
| `sync_to_postgres` silencieux en local | `NEON_DATABASE_URL` pas chargé | Vérifier que la variable est dans `.env` à la racine du projet |
| Données figées malgré sync | Cache Streamlit (`ttl=3600`) | Attendre 1h ou vider le cache via "⋮ → Clear cache" dans l'app |
| `psycopg2.OperationalError` | URL Neon incorrecte ou projet suspendu | Vérifier l'URL et l'état du projet sur neon.tech |

---

## PARTIE 12 — Checklist de vérification complète

```bash
# 1. Base de données
python -c "
import sqlite3
conn = sqlite3.connect('data/weather.db')
r = conn.execute('SELECT COUNT(*), COUNT(DISTINCT city), MIN(date), MAX(date) FROM weather_raw').fetchone()
p = conn.execute('SELECT COUNT(*), COUNT(DISTINCT city) FROM weather_predictions').fetchone()
print('DB brutes  :', r[0], 'lignes |', r[2], '->', r[3], '| villes:', r[1])
print('Prédictions:', p[0], 'lignes | villes:', p[1])
conn.close()
"

# 2. Modèles
python -c "
import os, json
pkls = [f for f in os.listdir('models') if f.endswith('.pkl') and 'mappings' not in f]
print('Modèles :', len(pkls), '/ 6')
with open('models/metrics.json') as f:
    m = json.load(f)
print('rain AUC  :', m.get('rain_tomorrow', {}).get('auc_roc', 'N/A'))
print('temp R²   :', m.get('max_temp_tomorrow', {}).get('r2', 'N/A'))
"

# 3. CSV
python -c "
rows = sum(1 for _ in open('data/output/weather_final.csv')) - 1
print('CSV       :', rows, 'lignes')
"

# 4. API
curl -s http://localhost:8001/health

# 5. Tests
python -m pytest tests/ -q
```

---

## Schéma de flux complet

```
INSTALLATION (une fois)
    git clone → venv → pip install → cp .env
          |
          v
BACKFILL (une fois, CLI ou DAG manuel)
    fetch 2008-2026 → repair gaps → train 6 modèles → predict → export CSV
          |
          v
SERVICES (démarrer manuellement ou via gestionnaire de processus)
    python api/app.py          → :8001  (FastAPI, local)
    streamlit run app.py       → :8501  (Dashboard)
    mlflow ui                  → :5000  (Tracking)
    docker compose up          → :8000/:9090/:3000/:8083 (API Docker/Prometheus/Grafana/Airflow)
    bash start_airflow.sh      → :8083  (Orchestrateur, mode dev)
          |
          v
CYCLE QUOTIDIEN (Airflow automatique)
    06:00 UTC — weather_daily_ingestion
        fetch J-1 → check qualité (≥ 21/26 villes) → predict → export → sync_to_postgres
              |
              └──────────────────────────────────────► Neon PostgreSQL → Render API → Streamlit
    09:00 UTC — weather_daily_monitoring
        qualité → drift KS → métriques 30j → décision 4 voies
          |
          v
CYCLE HEBDOMADAIRE (Airflow automatique)
    Lundi 02:00 UTC — weather_weekly_train
        snapshot baseline → retrain 6 modèles → compare baseline
        → rollback si dégradé sinon predict → export → MLflow log
          |
          v
CONSOMMATION
    Power BI  → http://localhost:8001/api/weather
    Streamlit → http://localhost:8501
    CSV       → data/output/weather_final.csv
    MLflow    → http://localhost:5000
```
