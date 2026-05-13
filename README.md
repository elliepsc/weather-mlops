# Australia Weather MLOps

Projet MLOps complet pour collecter, stocker, entrainer, monitorer et exposer des predictions meteo sur 26 villes australiennes.

La source meteo est Open-Meteo (ERA5-Land, gratuit et sans cle API). Les donnees alimentent une base SQLite, des modeles XGBoost suivis avec MLflow, une API FastAPI, un dashboard Streamlit, une couche analytics DuckDB/dbt et des exports Power BI.

## Sommaire

- [Architecture](#architecture)
- [Demarrage rapide](#demarrage-rapide)
- [Pipeline ML](#pipeline-ml)
- [API et dashboard](#api-et-dashboard)
- [Airflow](#airflow)
- [Analytics et Power BI](#analytics-et-power-bi)
- [Monitoring](#monitoring)
- [Structure du projet](#structure-du-projet)
- [Tests](#tests)
- [Documentation](#documentation)

## Architecture

```text
Open-Meteo API
    |
    v
pipeline/fetch_weather.py
    |
    v
data/weather.db (SQLite)
    |
    +-- pipeline/process_weather.py  -> feature engineering + labels
    +-- pipeline/train_models.py     -> 6 modeles XGBoost + MLflow
    +-- pipeline/predict.py          -> predictions meteo
    +-- pipeline/run_pipeline.py     -> orchestration CLI
    |
    +-- api/app.py                   -> FastAPI + /metrics Prometheus
    +-- streamlit_app/app.py         -> dashboard
    |
    +-- analytics/scripts/load_sources.py
             |
             v
         data/analytics.duckdb
             |
             v
         dbt run / dbt test
             |
             +-- data/analytics/*.csv       -> Power BI
             +-- pipeline/export_to_gcs.py  -> GCS Parquet optionnel
```

Composants principaux :

| Composant | Role |
|---|---|
| Open-Meteo | Source meteo gratuite, sans cle API. Archive historique et donnees recentes. |
| SQLite | Base locale `data/weather.db` avec `weather_raw`, `weather_predictions` et `v_weather_full`. |
| XGBoost | 6 modeles pour pluie, temperature, type meteo, canicule, gel et orage. |
| MLflow | Tracking local des runs, metriques et artefacts. |
| FastAPI | API JSON/CSV, endpoints analytics et metriques Prometheus. |
| Streamlit | Dashboard local consomme via l'API. |
| Airflow | Ingestion quotidienne, entrainement hebdomadaire, monitoring, backfill et analytics dbt. |
| DuckDB + dbt | Couche analytique locale pour Power BI et exploration SQL. |
| Prometheus + Grafana | Monitoring API via Docker Compose. |
| GCS + BigQuery | Migration optionnelle des exports analytics vers le cloud. |

## Demarrage rapide

Prerequis :

- Python 3.11 recommande
- Git
- Docker Desktop optionnel, pour API conteneurisee, Prometheus, Grafana et Airflow
- Power BI Desktop optionnel
- DBeaver et driver DuckDB ODBC optionnels

```bash
git clone https://github.com/elliepsc/meteo.git weather-mlops
cd weather-mlops

python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux / macOS / WSL
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
```

Open-Meteo ne demande pas de cle API. Pour le fonctionnement local de base, aucune variable secrete n'est obligatoire.

Premier chargement complet :

```bash
python pipeline/run_pipeline.py backfill
```

Puis lancer l'API :

```bash
python api/app.py
```

URLs locales :

| Service | URL |
|---|---|
| FastAPI | http://localhost:8001 |
| Swagger | http://localhost:8001/docs |
| Streamlit | http://localhost:8501 |
| MLflow UI | http://localhost:5000 |
| API Docker | http://localhost:8000 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Airflow UI | http://localhost:8083 |

## Configuration

Copier `.env.example` vers `.env`, puis ajuster uniquement ce qui est necessaire.

Variables les plus utiles :

| Variable | Defaut | Usage |
|---|---:|---|
| `ENV` | `dev` | Environnement logique. |
| `DB_PATH` | `data/weather.db` | Base SQLite meteo. |
| `API_HOST` | `0.0.0.0` | Host FastAPI. |
| `API_PORT` | `8001` | Port local FastAPI. Docker utilise `8000`. |
| `SLACK_WEBHOOK_URL` | vide | Alerting Slack optionnel. |
| `ALERT_EMAIL` | exemple | Email optionnel pour Airflow. |
| `GH_TOKEN` | vide | Token GitHub si un DAG doit pousser des exports CSV. |
| `GCS_ENABLED` | `false` | Active l'export Parquet vers GCS. |
| `GCP_PROJECT_ID` | vide | Projet GCP pour BigQuery/GCS. |
| `GCS_BUCKET` | vide | Bucket GCS cible. |

Les seuils metier et MLOps sont dans `config/mlops.yaml`. Les features, labels et hyperparametres sont dans `config/modeling.yaml`.

## Pipeline ML

Le point d'entree CLI est `pipeline/run_pipeline.py`.

| Commande | Effet |
|---|---|
| `backfill` | Init DB, historique 2008 -> J-1, entrainement, predictions, export CSV. |
| `backfill --force` | Recharge toutes les villes, meme si des donnees existent deja. |
| `daily` | Ingestion J-1, predictions et export. |
| `train` | Reentrainement complet, predictions et export. |
| `predict` | Regenere les predictions avec les modeles existants. |
| `repair --start-date ... --end-date ...` | Repare une periode manquante ou incomplete. |
| `export` | Exporte `v_weather_full` vers `data/output/weather_final.csv`. |

Exemples :

```bash
python pipeline/run_pipeline.py backfill
python pipeline/run_pipeline.py daily
python pipeline/run_pipeline.py train
python pipeline/run_pipeline.py repair --start-date 2026-04-01 --end-date 2026-04-23
python pipeline/run_pipeline.py export
```

Le backfill applique un delai entre villes pour respecter le free tier Open-Meteo. En cas de rate limit, relancer la commande sans `--force` : les donnees deja presentes ne sont pas rechargees.

Predictions produites :

| Colonne | Description |
|---|---|
| `rain_tomorrow` | Pluie demain, classification binaire. |
| `rain_tomorrow_proba` | Probabilite de pluie. |
| `max_temp_tomorrow` | Temperature max prevue demain. |
| `weather_type_tomorrow` | Type meteo : `Sunny`, `Cloudy`, `Rainy`, `Stormy`. |
| `comfort_score` | Score meteo de confort de 0 a 100. |
| `heatwave_risk` | Risque de canicule. |
| `frost_risk` | Risque de gel. |
| `storm_probability` | Probabilite d'orage. |

## API et dashboard

Lancement local :

```bash
python api/app.py
```

Endpoints principaux :

| Endpoint | Description |
|---|---|
| `GET /health` | Etat API et presence de la base SQLite. |
| `GET /api/cities` | Liste des villes et coordonnees. |
| `GET /api/weather` | Historique + predictions, filtrable par ville/date. |
| `GET /api/weather/latest` | Derniere ligne disponible par ville. |
| `GET /api/weather/predictions` | Predictions uniquement. |
| `GET /api/export/csv` | Telechargement de `weather_final.csv`. |
| `GET /api/analytics/{mart}` | Donnees d'un mart analytics en JSON. |
| `GET /api/analytics/{mart}.csv` | Donnees d'un mart analytics en CSV. |
| `GET /api/mlflow/runs` | Derniers runs MLflow. |
| `GET /api/mlflow/metrics` | Metriques depuis `models/metrics.json`. |
| `GET /metrics` | Metriques Prometheus. |

Exemple :

```bash
curl "http://localhost:8001/api/weather/latest?city=Sydney"
```

Dashboard Streamlit :

```bash
streamlit run streamlit_app/app.py
```

Le dashboard attend l'API sur `http://localhost:8001`.

## MLflow

```bash
# WSL avec repo sous /mnt/...
mlflow ui --backend-store-uri sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db --port 5000

# Windows natif ou Docker
mlflow ui --backend-store-uri sqlite:///mlflow/mlflow.db --port 5000
```

L'experience par defaut est `weather_australia`.

## Airflow

Les DAGs sont dans `dags/`.

| DAG | Schedule | Role |
|---|---|---|
| `weather_daily_ingestion` | `0 6 * * *` | Ingestion J-1, controle qualite, predictions, export. |
| `weather_gap_monitor` | `30 7 * * *` | Detection et reparation des trous de donnees. |
| `weather_daily_monitoring` | `0 9 * * *` | Drift, metriques, alertes et decision MLOps. |
| `weather_dbt_analytics` | `30 10 * * *` | Chargement DuckDB, `dbt run`, `dbt test`, exports analytics. |
| `weather_weekly_train` | `0 2 * * 1` | Reentrainement hebdomadaire avec validation baseline. |
| `weather_backfill` | manuel | Backfill historique parametrable. |

Docker Compose lance Airflow avec PostgreSQL :

```bash
docker compose up -d
```

UI Airflow : http://localhost:8083

Airflow 3.x utilise `api-server` au lieu de l'ancien `webserver`. En cas de changement de volumes ou variables Docker, recreer les services :

```bash
docker compose up -d --force-recreate
```

## Analytics et Power BI

La couche analytics charge SQLite et les JSON de monitoring vers DuckDB, puis construit des marts dbt pour Power BI.

```bash
make analytics-install
make analytics-all
```

Commandes detaillees :

```bash
python analytics/scripts/load_sources.py
cd analytics && dbt deps && dbt run --no-partial-parse && dbt test --no-partial-parse
python analytics/scripts/export_powerbi.py
```

Exports :

| Sortie | Chemin |
|---|---|
| DuckDB | `data/analytics.duckdb` |
| CSV Power BI | `data/analytics/*.csv` |
| Power Query template | `analytics/data_viz/template.pq` |
| Power BI project | `analytics/data_viz/weather_mlops.pbip` |
| GCS Parquet optionnel | `gs://<bucket>/weather-mlops/marts/...` |

Familles de marts dbt :

| Dossier | Contenu |
|---|---|
| `analytics/models/marts/weather` | Marts meteo propres, mensuels, annuels, extremes, alertes, confort. |
| `analytics/models/marts/forecast` | Predictions vs reel, accuracy mensuelle, matrice de confusion, calibration. |
| `analytics/models/marts/model` | Performance modele, calibration, biais, saisonnalite, extremes. |
| `analytics/models/marts/operations` | Sante MLOps, historique retrain, fraicheur des donnees. |
| `analytics/models/marts/drift` | Drift features et anomalies climatiques. |
| `analytics/models/marts/quality` | Qualite et completude quotidienne. |
| `analytics/models/marts/bi` | Tables simplifiees pour Power BI. |

Options Power BI :

| Option | Usage |
|---|---|
| CSV local | Importer les fichiers `data/analytics/*.csv`. |
| DuckDB ODBC | Connecter Power BI a `data/analytics.duckdb`. |
| API FastAPI | Lire `http://localhost:8001/api/weather` ou `/api/analytics/{mart}`. |
| BigQuery | Utiliser les external tables creees depuis les Parquet GCS. |

Voir [ANALYTICS.md](ANALYTICS.md), [analytics/data_viz/SOURCES.md](analytics/data_viz/SOURCES.md) et [docs/GCP_MIGRATION.md](docs/GCP_MIGRATION.md).

## Monitoring

Docker Compose demarre l'API, Prometheus, Grafana et Airflow :

```bash
docker compose up -d
```

| Service | URL | Notes |
|---|---|---|
| API FastAPI | http://localhost:8000 | Port Docker. |
| Prometheus | http://localhost:9090 | Scrape `/metrics` toutes les 15 s. |
| Grafana | http://localhost:3000 | Identifiants `admin / admin`. |
| Airflow UI | http://localhost:8083 | Airflow 3.x. |

Le DAG `weather_daily_monitoring` produit notamment :

```text
data/monitoring/model_metrics.json
data/monitoring/drift_report.json
data/monitoring/monitoring_decision.json
```

Decision MLOps :

| Decision | Condition typique |
|---|---|
| `trigger_retrain` | Drift fort ou degradation de performance, hors cooldown. |
| `alert_only` | Drift notable mais pas suffisant pour reentrainer. |
| `alert_insufficient_data` | Trop peu de donnees recentes pour conclure. |
| `no_action` | Modele stable. |

## Structure du projet

```text
weather-mlops/
|-- api/                         # FastAPI
|-- analytics/                   # dbt, DuckDB, exports Power BI
|-- config/                      # Config typed settings, modeling, MLOps policy
|-- dags/                        # Airflow DAGs
|-- data/                        # Donnees generees, non versionnees
|-- docs/                        # Guides complementaires
|-- monitoring/                  # Prometheus et Grafana
|-- models/                      # Modeles, metriques, feature importances
|-- notebooks/                   # Exploration
|-- pipeline/                    # Ingestion, processing, training, prediction, exports
|-- streamlit_app/               # Dashboard
|-- tests/                       # Tests unitaires et integration
|-- Dockerfile                   # Image API
|-- Dockerfile.airflow           # Image Airflow
|-- docker-compose.yaml          # API + Prometheus + Grafana + Airflow
|-- Makefile                     # Raccourcis dev/analytics/docker
|-- RUNBOOK.md                   # Guide operationnel complet
|-- ANALYTICS.md                 # Documentation analytics
`-- requirements.txt
```

## Tests

```bash
python -m pytest tests/ -q
```

Raccourcis Make :

```bash
make test
make lint
make format-check
make ci
```

Les tests couvrent notamment le feature engineering, Open-Meteo, XGBoost, MLflow, les DAGs Airflow, le monitoring, le backfill et l'ingestion incrementale.

## Fichiers generes

Ces fichiers sont regenerables et ne doivent pas etre traites comme du code source :

```text
data/weather.db
data/output/weather_final.csv
data/analytics.duckdb
data/analytics/*.csv
data/monitoring/*.json
models/*.pkl
models/metrics.json
models/baseline/
mlflow/mlflow.db
analytics/target/
analytics/dbt_packages/
logs/pipeline.log
```

## Documentation

| Document | Contenu |
|---|---|
| [RUNBOOK.md](RUNBOOK.md) | Guide operationnel complet de bout en bout. |
| [ANALYTICS.md](ANALYTICS.md) | Architecture analytics, DuckDB, dbt et Power BI. |
| [docs/GCP_MIGRATION.md](docs/GCP_MIGRATION.md) | Migration GCS + BigQuery external tables. |
| [docs/ML_DOCUMENTATION.md](docs/ML_DOCUMENTATION.md) | Details modeles, features et evaluation. |
| [docs/backlog.md](docs/backlog.md) | Backlog projet. |

## Auteurs

Projet developpe dans le cadre de la formation Machine Learning Engineer de DataScientest.

Mentor : Sebastien SIME - [LinkedIn](https://www.linkedin.com/in/s-sime/) - [GitHub](https://github.com/ssime-git)
