# Australia Weather MLOps

Projet MLOps pour collecter, stocker, entraîner et exposer des prédictions météo sur 26 villes australiennes.
Les données proviennent de l'API Open-Meteo (ERA5-Land, résolution 9 km), couvrent la période **2008-2026** (~174 000 lignes), et alimentent 6 modèles XGBoost trackés via MLflow.
Les résultats sont exposés via FastAPI, un dashboard Streamlit, un export CSV et des endpoints Power BI.

---

## Architecture

```text
Open-Meteo API (ERA5-Land, archive 2008 + forecast J-1)
    |
    v
pipeline/fetch_weather.py        fetch_city() — archive ou forecast selon la date
    |                            délai 10 s entre villes (rate limit free tier)
    v
data/weather.db (SQLite)
    |
    +-- pipeline/process_weather.py  --> feature engineering (40+ features, labels)
    |
    +-- pipeline/train_models.py     --> 6 modèles XGBoost + MLflow tracking
    |
    +-- pipeline/predict.py          --> 8 colonnes de prédiction
    |
    +-- data/output/weather_final.csv  (vue v_weather_full exportée)
    |
    +-- api/app.py                   --> FastAPI :8001 (local) / :8000 (Docker)
    |        |
    |        +-- streamlit_app/app.py      dashboard interactif
    |        +-- Prometheus /metrics
    |
    +-- analytics/scripts/load_sources.py  --> SQLite + JSON → DuckDB
             |
             v
         data/analytics.duckdb
             |
             v
         dbt run (analytics/models/)
             |
             +-- main_marts.*  → data/analytics/*.csv → Power BI Import
             +-- main_marts.*  → ODBC driver          → Power BI Live
```

**Composants :**

| Composant | Rôle |
|---|---|
| **Open-Meteo** | Source météo gratuite, sans clé API. ERA5-Land 9 km. Lag ~1 jour. |
| **SQLite** | Base locale `data/weather.db`. Tables `weather_raw` + `weather_predictions` + vue `v_weather_full`. |
| **XGBoost** | 6 modèles sauvegardés dans `models/`. Paramètres dans `config/modeling.yaml`. |
| **MLflow** | Tracking local SQLite. Sous WSL avec repo sur `/mnt/...`, le backend bascule automatiquement vers `~/.weather-mlops/mlflow`. |
| **FastAPI** | Endpoints JSON/CSV + métriques Prometheus. Port 8001 (local) ou 8000 (Docker). |
| **Streamlit** | Dashboard local connecté à l'API. |
| **Airflow** | Orchestration : ingestion quotidienne, réentraînement hebdomadaire, monitoring, backfill, gap monitoring. |
| **Prometheus/Grafana** | Monitoring API via Docker Compose. |
| **DuckDB + dbt** | Couche analytique dans `analytics/`. `load_sources.py` charge SQLite → DuckDB, `dbt run` construit 5 marts Power BI. Adapter BigQuery prêt. |
| **DBeaver** | SQL client GUI (connexion native DuckDB v23+). Installation séparée : https://dbeaver.io |
| **Power BI** | Connexion via ODBC (live) ou import CSV (`data/analytics/*.csv`). Voir `ANALYTICS.md`. |

---

## Source de données

### Open-Meteo (ERA5-Land)

- URL : https://open-meteo.com
- Gratuit, sans clé API
- Résolution : ~9 km² (ERA5-Land, downscalé depuis ERA5 à 31 km)
- Lag : ~1 jour (données de la veille disponibles)
- Endpoints utilisés :
  - `archive-api.open-meteo.com/v1/archive` — données historiques (2008 → J-2)
  - `api.open-meteo.com/v1/forecast` — données récentes (J-90 → J-1)

**Variables récupérées :**

| Variable Open-Meteo | Colonne DB | Notes |
|---|---|---|
| `temperature_2m_min/max` | `min_temp`, `max_temp` | °C |
| `precipitation_sum` | `rainfall` | mm, minuit → minuit |
| `et0_fao_evapotranspiration` | `evaporation` | mm |
| `sunshine_duration` | `sunshine_hours` | secondes → heures |
| `windgusts_10m_max` | `wind_gust_speed` | km/h |
| `winddirection_10m_dominant` | `wind_gust_dir` | degrés → boussole 16 pts |
| `weathercode` | `weather_code` | code WMO |
| `temperature_2m` (horaire 9h/15h) | `temp_9am`, `temp_3pm` | °C |
| `relative_humidity_2m` (horaire 9h/15h) | `humidity_9am`, `humidity_3pm` | % |
| `pressure_msl` (horaire 9h/15h) | `pressure_9am`, `pressure_3pm` | hPa, niveau mer |
| `cloudcover` (horaire 9h/15h) | `cloud_9am`, `cloud_3pm` | % → oktas (0-8) |
| `windspeed_10m` (horaire 9h/15h) | `wind_speed_9am`, `wind_speed_3pm` | km/h |

**Champs enrichis (stockés, pas encore features modèles) :**

| Variable Open-Meteo | Colonne DB | Notes |
|---|---|---|
| `rain_sum` | `rain_sum` | mm (pluie liquide uniquement) |
| `precipitation_hours` | `precipitation_hours` | heures de précipitation |
| `dew_point_2m` (horaire 9h/15h) | `dew_point_9am`, `dew_point_3pm` | °C |
| `surface_pressure` (horaire 9h/15h) | `surface_pressure_9am`, `surface_pressure_3pm` | hPa (niveau station) |

### Qualité des données vs BOM

Les données Open-Meteo correspondent bien aux stations BOM (mesures terrain) :

| Métrique | Delta typique Open-Meteo vs BOM | Commentaire |
|---|---|---|
| min_temp / max_temp | ±1-3°C | Biais systématique : ERA5-Land lisse les extrêmes de cuvette/asphalte |
| rainfall | ±0-2 mm (jours secs) | Écart possible sur jours de forte pluie (période de mesure différente) |
| rafales vent | ±4 km/h | Très proche |
| pression MSL | ±2 hPa | Excellent |
| humidity_9am | ±5-10% | Snapshot 9h vs moyenne journalière BOM |

> **Note :** Le biais de température est structurel (grille 9 km² vs station ponctuelle). Il est cohérent sur toute la période 2008-2026, donc n'impacte pas la qualité du modèle ML qui apprend les patterns Open-Meteo de bout en bout.

---

## Données

### Couverture

- **Période** : 2008-01-01 → hier (J-1)
- **Villes** : 26 villes australiennes
- **Lignes** : ~173 800 (26 × ~6 686 jours)
- **Colonnes brutes** : 35 (météo de base + champs enrichis) + 8 (prédictions) dans `weather_final.csv`

### Villes couvertes (26)

| État | Villes |
|---|---|
| NSW | Albury, Newcastle, Sydney, WaggaWagga, Wollongong |
| VIC | Ballarat, Bendigo, Melbourne, Mildura |
| QLD | Brisbane, Cairns, GoldCoast, Townsville |
| SA | Adelaide, MountGambier, Nuriootpa, Woomera |
| WA | Albany, Perth |
| TAS | Hobart, Launceston |
| NT | AliceSprings, Darwin, Katherine |
| ACT | Canberra, Tuggeranong |

---

## Prédictions produites

| Colonne | Type | Modèle | Description |
|---|---|---|---|
| `rain_tomorrow` | Binaire 0/1 | XGBoost classification | Pluie demain (seuil : >1 mm) |
| `rain_tomorrow_proba` | Probabilité 0-1 | XGBoost classification | Probabilité de pluie |
| `max_temp_tomorrow` | Régression | XGBoost régression | Température max prévue demain (°C) |
| `weather_type_tomorrow` | Multi-classe | XGBoost multiclass | `Sunny`, `Cloudy`, `Rainy`, `Stormy` |
| `comfort_score` | Score 0-100 | Formule | Température 18-24°C idéale, pénalités humidité/vent/pluie |
| `heatwave_risk` | Probabilité 0-1 | XGBoost classification | Risque canicule (≥35°C deux jours consécutifs) |
| `frost_risk` | Probabilité 0-1 | XGBoost classification | Risque gel (min_temp ≤ 2°C) |
| `storm_probability` | Probabilité 0-1 | XGBoost classification | Probabilité d'orage (codes WMO 95-99 ou pluie > 10mm + rafales > 50 km/h) |

### Métriques des modèles (entraînement sur ~173 800 lignes, 2008-2026)

| Modèle | Accuracy / MAE | R² | AUC-ROC |
|---|---|---|---|
| `rain_tomorrow` | 77.0% | — | **0.852** |
| `max_temp_tomorrow` | MAE 1.63°C | **0.907** | — |
| `weather_type_tomorrow` | 82.2% | — | — |
| `heatwave_risk` | — | — | **0.996** |
| `frost_risk` | — | — | **0.989** |
| `storm_probability` | — | — | **0.898** |

---

## Feature engineering

`pipeline/process_weather.py` produit 40+ features à partir des données brutes :

- **Calendrier** : mois, jour de l'année, saison (hémisphère sud : déc-fév = été)
- **Lags** (par ville) : `max_temp_lag1/2`, `min_temp_lag1`, `rainfall_lag1/2`, `pressure_3pm_lag1`
- **Moyennes glissantes 7 jours** : `max_temp_rolling7`, `rainfall_rolling7`, `humidity_3pm_rolling7`
- **Dérivés** :
  - `pressure_tendency` = pression_3pm - pression_3pm_lag1 (chute → tempête)
  - `temp_anomaly` = max_temp - max_temp_rolling7 (anomalie thermique)
  - `consec_hot_days` = cumul de jours chauds (détection canicule)
  - `comfort_score` = formule basée sur temp, humidité, vent, soleil
- **Labels cibles** (décalés J+1 par ville) : `rain_tomorrow`, `max_temp_tomorrow`, `weather_type_tomorrow`, `heatwave_risk`, `frost_risk`, `storm_label`

---

## Structure du projet

```text
weather-mlops/
├── api/
│   └── app.py                    # FastAPI — 9 endpoints + Prometheus
├── dags/
│   ├── ingestion_dag.py          # Ingestion quotidienne (06:00 UTC)
│   ├── train_dag.py              # Réentraînement hebdomadaire (lundi 02:00) + baseline gate
│   ├── monitoring_dag.py         # Qualité, couverture, drift, métriques (09:00 UTC)
│   ├── backfill_dag.py           # Backfill manuel paramétrable
│   ├── gap_monitor_dag.py        # Détection et réparation des trous de données
│   ├── _airflow_compat.py        # Couche de compatibilité Airflow 3.x pour les tests
│   └── _notifications.py         # Helpers alertes Slack
├── pipeline/
│   ├── locations.py              # 26 villes (lat, lon, timezone, state)
│   ├── fetch_weather.py          # Appels Open-Meteo (archive + forecast)
│   ├── database.py               # Schéma SQLite, upserts, vue v_weather_full
│   ├── process_weather.py        # Feature engineering + labels
│   ├── train_models.py           # Entraînement XGBoost + MLflow
│   ├── predict.py                # Génération des prédictions
│   ├── mlflow_config.py          # Résolution URI MLflow (Windows / WSL / Docker)
│   └── run_pipeline.py           # CLI d'orchestration
├── streamlit_app/
│   ├── app.py                    # Dashboard Streamlit
│   └── requirements.txt
├── tests/
│   ├── test_preprocess.py
│   ├── test_xgboost_model.py
│   ├── test_mlflow_config.py
│   ├── test_fetch_weather.py
│   ├── test_modeling_config.py
│   ├── test_monitoring_branch.py
│   ├── test_ingestion_backfill_flow.py
│   └── test_train_dag.py
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       └── provisioning/
├── analytics/
│   ├── dbt_project.yml           # Config dbt — staging=views, marts=tables
│   ├── profiles.yml              # DuckDB (défaut) + BigQuery (target bigquery)
│   ├── packages.yml              # dbt_utils >= 1.0
│   ├── macros/
│   │   └── datediff_days.sql     # Macro cross-adapter DuckDB / BigQuery
│   ├── models/
│   │   ├── staging/              # Typage + renommage 1:1 (vues)
│   │   ├── core/                 # dim_cities (table)
│   │   ├── intermediate/         # Jointures + calculs partagés (tables)
│   │   └── marts/                # Tables finales Power BI (tables)
│   └── scripts/
│       ├── load_sources.py       # SQLite + JSON monitoring → DuckDB
│       ├── export_powerbi.py     # DuckDB marts → data/analytics/*.csv
│       ├── setup_odbc_dsn.ps1    # Enregistre DSN Windows pour Power BI ODBC (Admin)
│       └── powerbi_datasource.py # Blocs Python pour Power BI → Get Data → Python
├── notebooks/
│   └── 01_exploration.ipynb      # Exploration DuckDB : qualité, perf, villes, drift
├── data/
│   ├── weather.db                # SQLite (weather_raw + weather_predictions)
│   ├── analytics.duckdb          # DuckDB analytics (généré par load_sources.py)
│   ├── powerbi/                  # CSV exports pour Power BI (générés par export_powerbi.py)
│   └── output/
│       └── weather_final.csv     # Vue complète exportée (~174 000 lignes)
├── models/                       # Modèles .pkl + metrics.json + feature importances
│   └── baseline/                 # Snapshot du dernier train validé (gate de rollback)
├── mlflow/                       # Tracking MLflow local (Windows / Docker)
├── config/
│   ├── mlops.yaml               # Seuils MLOps, gating, cooldowns
│   ├── modeling.yaml            # Hyperparamètres XGBoost, features, labels
│   └── settings.py              # Chargement typé des configs
├── docker-compose.yaml           # API + Prometheus + Grafana + Airflow (PostgreSQL)
├── Dockerfile                    # Image Python 3.11 slim pour l'API (port 8000)
├── Dockerfile.airflow            # Image apache/airflow:3.0.0 + dépendances pipeline
├── start_airflow.sh              # Lanceur Airflow standalone (mode dev / WSL)
└── requirements.txt
```

---

## Installation locale

**Prérequis :**
- Python 3.10+
- Git
- Docker Desktop (optionnel, pour Prometheus/Grafana/Airflow)
- Apache Airflow (optionnel, pour l'orchestration planifiée)
- DBeaver (optionnel, SQL GUI pour DuckDB) : https://dbeaver.io
- DuckDB ODBC driver (optionnel, connexion live Power BI) : https://duckdb.org/docs/api/odbc/overview

```bash
git clone https://github.com/elliepsc/meteo.git weather-mlops
cd weather-mlops

python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

Open-Meteo ne demande pas de clé API.

---

## Pipeline CLI

Le point d'entrée est `pipeline/run_pipeline.py`.

### Commandes disponibles

| Commande | Effet |
|---|---|
| `backfill` | Premier lancement : init DB, historique 2008 → J-1, entraînement, prédictions, export |
| `backfill --force` | Idem en re-téléchargeant toutes les villes (ignore les données existantes) |
| `daily` | Ingestion J-1 + prédictions + export (tâche Airflow quotidienne) |
| `train` | Réentraînement complet + prédictions + export (tâche Airflow hebdomadaire) |
| `predict` | Régénération des prédictions avec les modèles existants (sans réentraîner) |
| `repair --start-date ... --end-date ...` | Répare les dates manquantes ou incomplètes sur un intervalle |
| `export` | Export de `v_weather_full` vers `data/output/weather_final.csv` |

```bash
# Premier lancement complet (~15 min, 26 villes, 18 ans)
python pipeline/run_pipeline.py backfill

# Mise à jour quotidienne
python pipeline/run_pipeline.py daily

# Réentraîner les modèles
python pipeline/run_pipeline.py train

# Réparer un intervalle avec trous / lignes incomplètes
python pipeline/run_pipeline.py repair --start-date 2026-04-01 --end-date 2026-04-23

# Exporter le CSV uniquement
python pipeline/run_pipeline.py export
```

### Rate limiting Open-Meteo

L'API gratuite Open-Meteo impose une limite de taux. Le pipeline applique automatiquement :
- **10 secondes de délai** entre chaque ville lors du backfill
- **Retry exponentiel** en cas de 429 : 30 s → 60 s → 120 s (3 tentatives)
- **Résumé** : le backfill complet prend ~15 minutes pour 26 villes (18 ans de données)

En cas d'échec partiel (certaines villes en erreur), relancer sans `--force` pour ne re-télécharger que les villes manquantes :

```bash
python pipeline/run_pipeline.py backfill
```

---

## Schéma de la base de données

### Table `weather_raw`

Données brutes Open-Meteo, une ligne par (date, ville).

| Colonne | Type | Description |
|---|---|---|
| date | TEXT | YYYY-MM-DD |
| city | TEXT | Nom de la ville (clé LOCATIONS) |
| state | TEXT | État australien |
| latitude / longitude | REAL | Coordonnées GPS |
| min_temp / max_temp | REAL | °C |
| rainfall | REAL | mm (minuit → minuit) |
| evaporation | REAL | mm (ET0 FAO) |
| sunshine_hours | REAL | heures |
| wind_gust_speed | REAL | km/h |
| wind_gust_dir | TEXT | Boussole 16 points |
| wind_speed_9am / 3pm | REAL | km/h |
| wind_dir_9am / 3pm | TEXT | Boussole 16 points |
| humidity_9am / 3pm | REAL | % |
| pressure_9am / 3pm | REAL | hPa (niveau mer) |
| cloud_9am / 3pm | REAL | oktas (0-8) |
| temp_9am / 3pm | REAL | °C |
| rain_today | INTEGER | 1 si rainfall > 1 mm |
| weather_code | INTEGER | Code WMO |
| rain_sum | REAL | mm (pluie liquide uniquement) |
| precipitation_hours | REAL | heures de précipitation |
| dew_point_9am / 3pm | REAL | °C (point de rosée) |
| surface_pressure_9am / 3pm | REAL | hPa (pression station, pas MSL) |

### Table `weather_predictions`

Prédictions générées par les 6 modèles, une ligne par (date, ville).

### Vue `v_weather_full`

`LEFT JOIN weather_raw + weather_predictions` — table unifiée pour l'API et l'export CSV.

---

## API FastAPI

```bash
# Local (port 8001 par défaut)
python api/app.py
```

URL locale : `http://localhost:8001` — Swagger : `http://localhost:8001/docs`

Via Docker Compose, l'API tourne sur le port **8000**.

| Endpoint | Description |
|---|---|
| `GET /health` | État de l'API et présence de la base SQLite |
| `GET /api/cities` | Liste des 26 villes et coordonnées |
| `GET /api/weather` | Données historiques + prédictions, filtrables par ville/date |
| `GET /api/weather/latest` | Dernière ligne disponible par ville |
| `GET /api/weather/predictions` | Prédictions uniquement (payload allégé) |
| `GET /api/export/csv` | Téléchargement de `weather_final.csv` |
| `GET /api/mlflow/runs` | Derniers runs MLflow |
| `GET /api/mlflow/metrics` | Métriques depuis `models/metrics.json` |
| `GET /metrics` | Métriques Prometheus |

```bash
curl "http://localhost:8001/api/weather/latest?city=Sydney"
```

---

## Dashboard Streamlit

Le dashboard consomme l'API FastAPI sur `http://localhost:8001`.
Démarrer l'API en premier, puis :

```bash
streamlit run streamlit_app/app.py
```

Affiche : dernières prédictions par ville, tendances historiques, métriques du dernier entraînement.

---

## Analytics — dbt + DuckDB

La couche analytique transforme les données SQLite brutes en marts Power BI.
Documentation complète : [ANALYTICS.md](ANALYTICS.md)

### Démarrage rapide

```bash
# Installer les dépendances analytics
make analytics-install

# Pipeline complet : SQLite → DuckDB → dbt → CSV Power BI
make analytics-all
```

### Explorer les données

```bash
# CLI DuckDB
duckdb data/analytics.duckdb
> SHOW SCHEMAS;
> SELECT * FROM main_marts.mart_mlops_health LIMIT 10;

# DBeaver : New Connection → DuckDB → path: data/analytics.duckdb
```

### Marts disponibles

| Mart | Grain | Contenu |
|---|---|---|
| `mart_model_performance_overview` | mois | Accuracy + MAE globaux |
| `mart_model_performance_by_city` | ville × mois | Dégradation par ville |
| `mart_forecast_vs_actual_timeline` | ville × jour (90j) | Prédictions vs réel |
| `mart_mlops_health` | jour (30j) | Complétude + décisions ops |
| `mart_retraining_history` | événement retrain | Audit avant/après retrain |

### Connexion Power BI

**Option A — Import CSV (recommandé) :**
```bash
make analytics-export
# → data/analytics/*.csv (un fichier par mart)
```
Dans Power BI Desktop : **Obtenir les données → Texte/CSV**

**Option B — Connexion live ODBC :**
1. Installer DuckDB ODBC driver : https://duckdb.org/docs/api/odbc/overview
2. Créer DSN → `data/analytics.duckdb`
3. Power BI : **Obtenir les données → ODBC** → Import mode

**Option C — API FastAPI (données brutes) :**
```text
http://localhost:8001/api/weather
http://localhost:8001/api/export/csv
```

---

## Power BI (données brutes via API)

**Connecteur Web :**

1. Ouvrir Power BI Desktop
2. **Obtenir les données > Web**
3. URL : `http://localhost:8001/api/weather` ou `.../api/weather/latest`
4. Dans Power Query, développer le champ `data`

**CSV brut :**

```text
data/output/weather_final.csv
```

Régénéré automatiquement par `backfill`, `daily`, `train` et `export`.

---

## MLflow

```bash
# WSL (repo sous /mnt/...)
mlflow ui --backend-store-uri sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db --port 5000

# Windows natif ou Docker
mlflow ui --backend-store-uri sqlite:///mlflow/mlflow.db --port 5000
```

URL locale : `http://localhost:5000`

Chaque entraînement crée un run parent (stats dataset) avec des runs enfants par modèle (params, métriques, artefacts).
L'expérience par défaut est `weather_australia` (définie dans `config/modeling.yaml`).

---

## Airflow

Les DAGs sont dans `dags/`.

| DAG | Schedule | Tâches principales |
|---|---|---|
| `weather_daily_ingestion` | `0 6 * * *` | init DB → fetch J-1 → check qualité → prédictions → export |
| `weather_weekly_train` | `0 2 * * 1` | snapshot baseline → retrain → compare baseline → rollback si dégradé sinon prédictions → export |
| `weather_daily_monitoring` | `0 9 * * *` | qualité → couverture → drift KS → métriques → décision 4 voies → retrain auto si nécessaire |
| `weather_backfill` | Manuel | fetch historique → repair gaps → retrain → prédictions → export |
| `weather_gap_monitor` | Planifié | détection et réparation des trous dans `weather_raw` |

### Logique de monitoring (4 voies)

Le DAG `weather_daily_monitoring` prend une décision parmi 4 branches :

| Décision | Condition |
|---|---|
| `trigger_retrain` | Drift KS ≥ 4 features OU accuracy pluie < 75 % (et cooldown expiré) |
| `alert_only` | Drift KS ≥ 3 features (probable saisonnalité, pas suffisant pour retraîner) |
| `alert_insufficient_data` | < 30 lignes disponibles pour calculer les métriques |
| `no_action` | Modèle stable |

Le cooldown est de **7 jours** entre deux retrains automatiques (configurable dans `config/mlops.yaml`).

### Baseline validation (train DAG)

Le DAG `weather_weekly_train` valide le nouveau modèle avant de le mettre en production :
- Si `rain_accuracy` chute de plus de 2 pp **ou** `temp_mae` augmente de plus de 0.2°C → rollback automatique vers `models/baseline/`
- Seuils dans `config/mlops.yaml` → `training.baseline_tolerance`

```bash
# Lancement standalone local (mode dev)
bash start_airflow.sh

# UI Airflow : http://localhost:8083
```

---

## Monitoring Docker

```bash
docker compose up -d
```

| Service | URL | Notes |
|---|---|---|
| API FastAPI | http://localhost:8000 | Port 8000 dans Docker (8001 en local) |
| Prometheus | http://localhost:9090 | Scrape `/metrics` toutes les 15 s |
| Grafana | http://localhost:3000 | Identifiants : admin / admin |
| Airflow UI | http://localhost:8083 | Identifiants dans les logs au 1er démarrage |

---

## Tests

```bash
python -m pytest tests/ -q
```

60 tests couvrant le feature engineering, les appels Open-Meteo enrichis, l'entraînement et la persistance des modèles, la config MLflow, la configuration YAML, la logique de branching du DAG monitoring, la gate baseline et le rollback du DAG train, l'idempotence de l'ingestion quotidienne et la reprise incrémentale du backfill.

---

## Fichiers générés (non versionnés)

```text
data/weather.db
data/output/weather_final.csv
data/analytics.duckdb          # régénéré par analytics/scripts/load_sources.py
data/analytics/*.csv           # régénéré par analytics/scripts/export_powerbi.py
data/monitoring/*.json
models/*.pkl
models/metrics.json
models/baseline/
mlflow/mlflow.db
~/.weather-mlops/mlflow/mlflow.db   # créé automatiquement sous WSL sur /mnt/...
logs/pipeline.log
analytics/target/              # SQL compilé auto-généré par dbt
analytics/dbt_packages/        # dépendances dbt (équivalent node_modules)
```

---

## Auteurs

Projet développé dans le cadre de la formation Machine Learning Engineer de DataScientest.

| Contributeur | LinkedIn | GitHub |
|---|---|---|
| Leila BELMIR | | |
| Anas MBARKI | | |
| Ellie PASCAUD | | |
| Sergio VELASCO | | |

Mentor : Sébastien SIME — [LinkedIn](https://www.linkedin.com/in/s-sime/) — [GitHub](https://github.com/ssime-git)
