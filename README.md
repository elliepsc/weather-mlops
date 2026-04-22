# australia-weather-mlops

Projet MLOps complet pour prédire les conditions météorologiques du lendemain sur 26 villes australiennes. Les données sont collectées quotidiennement via l'API Open-Meteo, 6 modèles XGBoost génèrent 8 colonnes de prédiction, et les résultats sont exposés via une API REST consommable directement par Power BI.

---

## 1. Architecture

```
Open-Meteo API
      │
      ▼
pipeline/fetch_weather.py      ← collecte quotidienne (26 villes)
      │
      ▼
data/weather.db (SQLite)       ← stockage central
      │
      ├──► pipeline/train_models.py  ← entraînement hebdomadaire (6 modèles XGBoost)
      │           │
      │           └──► mlflow/mlflow.db  ← tracking expériences + métriques + artefacts
      │
      ├──► pipeline/predict.py       ← génération des 8 colonnes de prédiction
      │
      └──► api/app.py (FastAPI :8080)
                │
                ├── /api/weather          → Power BI Web connector
                ├── /api/weather/latest   → dashboard temps réel
                ├── /api/mlflow/runs      → suivi des entraînements
                ├── /api/mlflow/metrics   → dernières métriques
                └── /api/export/csv       → data/output/weather_final.csv
```

**Orchestration :** Apache Airflow — 4 DAGs

**MLflow :** tracking URI SQLite local `mlflow/mlflow.db` — UI via `mlflow ui` sur `:5000`

---

> **Note :** Le diagramme d'architecture (`_readme/images/Image1.png`) correspond à l'ancienne version du projet (scraping BOM, Docker microservices, MLflow, DVC). À remplacer avec la nouvelle architecture Open-Meteo → SQLite → FastAPI → Power BI.

## 2. Prédictions produites

| Colonne | Type | Description |
|---|---|---|
| `rain_tomorrow` | Binaire (0/1) | Pluie demain ? |
| `rain_tomorrow_proba` | Probabilité 0–1 | Probabilité de pluie |
| `max_temp_tomorrow` | Régression (°C) | Température max prévue |
| `weather_type_tomorrow` | Multi-classe | Sunny / Cloudy / Rainy / Stormy |
| `comfort_score` | Score 0–100 | Indice de confort (temp + humidité + vent + soleil) |
| `heatwave_risk` | Probabilité 0–1 | Risque de canicule (≥3 jours consécutifs >35°C) |
| `frost_risk` | Probabilité 0–1 | Risque de gel (min_temp ≤ 2°C) |
| `storm_probability` | Probabilité 0–1 | Probabilité d'orage |

---

## 3. Structure du projet

```
australia-weather-mlops/
├── pipeline/
│   ├── locations.py        # 26 villes australiennes + coordonnées GPS
│   ├── fetch_weather.py    # Appels Open-Meteo API (historique + daily)
│   ├── database.py         # SQLite — schema, upsert, vue v_weather_full
│   ├── process_weather.py  # Feature engineering + construction des labels
│   ├── train_models.py     # Entraînement des 6 modèles XGBoost
│   ├── predict.py          # Génération des 8 colonnes de prédiction
│   └── run_pipeline.py     # Orchestrateur CLI
├── api/
│   └── app.py              # FastAPI — endpoints Power BI
├── src/
│   └── airflow/dags/
│       ├── ingestion_dag.py   # DAG quotidien (06h UTC)
│       ├── train_dag.py       # DAG hebdomadaire (lundi 02h UTC)
│       ├── monitoring_dag.py  # DAG monitoring quotidien (08h UTC)
│       └── backfill_dag.py    # DAG manuel — backfill configurable
├── data/
│   ├── weather.db             # Base SQLite (source principale Power BI)
│   ├── monitoring/            # drift_report.json, model_metrics.json
│   └── output/
│       └── weather_final.csv  # Export CSV (fallback Power BI)
├── models/                    # Modèles XGBoost sauvegardés (.pkl)
├── legacy/                    # Ancienne stack Docker (api_model, Gateway, ingest…)
├── logs/
│   └── pipeline.log
└── requirements.txt
```

---

## 4. Installation et premier lancement

### Prérequis
- Python 3.10+
- (Optionnel) Apache Airflow 2.8+ pour l'orchestration automatique

### Installation

```bash
git clone https://github.com/elliepsc/meteo.git australia-weather-mlops
cd australia-weather-mlops
python -m venv venv

# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
```

### Premier lancement — backfill 2 ans

Cette commande récupère 2 ans de données historiques, entraîne les 6 modèles XGBoost et génère les 8 colonnes de prédiction. À ne faire qu'une seule fois (~5–10 minutes).

```bash
python pipeline/run_pipeline.py backfill
```

### Lancer l'API Power BI

```bash
python api/app.py
# → http://localhost:8080
```

---

## 5. Connexion Power BI

### Option A — Web connector (recommandée, auto-refresh)

Dans Power BI Desktop :
1. **Obtenir les données → Web**
2. Coller l'URL : `http://localhost:8080/api/weather`
3. Power Query → naviguer dans `data` → **Développer en nouvelles lignes**
4. Configurer l'actualisation planifiée

Endpoints disponibles :

| URL | Contenu |
|---|---|
| `/api/weather` | Table complète (historique + prédictions) |
| `/api/weather/latest` | Dernière date par ville |
| `/api/weather/predictions` | Prédictions seules |
| `/api/export/csv` | Téléchargement CSV |

### Option B — Fichier CSV (fallback local)

Fichier mis à jour quotidiennement par le pipeline :
```
data/output/weather_final.csv
```

---

## 6. Orchestration Airflow

| DAG | Schedule | Tâches |
|---|---|---|
| `weather_daily_ingestion` | Tous les jours à 06h UTC | init_db → fetch_daily → run_predictions → export_csv |
| `weather_weekly_train` | Lundi à 02h UTC | retrain_models → run_predictions → export_csv |
| `weather_daily_monitoring` | Tous les jours à 08h UTC | data_quality → prediction_coverage → drift_detection → model_metrics → alert |
| `weather_backfill` | Manuel uniquement | fetch_historical (configurable) → retrain → predict → export |

### Lancer MLflow UI

```bash
mlflow ui --backend-store-uri sqlite:///mlflow/mlflow.db --port 5000
# → http://localhost:5000
```

Chaque entraînement crée un run parent avec les métriques agrégées et 6 runs enfants (un par modèle) avec hyperparamètres, métriques, feature importances et artefacts.

### Lancer Airflow localement

```bash
pip install apache-airflow
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create --username admin --password admin --role Admin \
    --firstname Admin --lastname Admin --email admin@example.com
airflow webserver --port 8080 &
airflow scheduler &
# → http://localhost:8080
```

### Mise à jour manuelle (sans Airflow)

```bash
python pipeline/run_pipeline.py daily    # mise à jour quotidienne
python pipeline/run_pipeline.py train    # retraining uniquement
python pipeline/run_pipeline.py predict  # prédictions uniquement
python pipeline/run_pipeline.py export   # export CSV uniquement
```

---

## 7. Source de données

**Open-Meteo API** — [open-meteo.com](https://open-meteo.com)
- Gratuite, sans clé API
- Données horaires et journalières pour n'importe quelle coordonnée GPS
- Archive historique disponible depuis 1940

Variables collectées : température min/max, précipitations, évapotranspiration, ensoleillement, vitesse et direction du vent, humidité, pression atmosphérique, couverture nuageuse — à 9h et 15h heure locale.

---

## 8. Auteurs

Projet développé dans le cadre de la formation Machine Learning Engineer de DataScientest

| Contributeur | LinkedIn | GitHub |
|---|---|---|
| Leila BELMIR | [LinkedIn]() | [GitHub]() |
| Anas MBARKI | [LinkedIn]() | [GitHub]() |
| Ellie PASCAUD | [LinkedIn]() | [GitHub]() |
| Sergio VELASCO | [LinkedIn]() | [GitHub]() |

**Mentor :** Sébastien SIME — [LinkedIn](https://www.linkedin.com/in/s-sime/) · [GitHub](https://github.com/ssime-git)
