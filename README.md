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
    +-- api/app.py                   --> FastAPI :8083
             |
             +-- streamlit_app/app.py      dashboard interactif
             +-- Power BI Web connector
             +-- Prometheus /metrics
```

**Composants :**

| Composant | Rôle |
|---|---|
| **Open-Meteo** | Source météo gratuite, sans clé API. ERA5-Land 9 km. Lag ~1 jour. |
| **SQLite** | Base locale `data/weather.db`. Tables `weather_raw` + `weather_predictions` + vue `v_weather_full`. |
| **XGBoost** | 6 modèles sauvegardés dans `models/`. Paramètres dans `config/modeling.yaml`. |
| **MLflow** | Tracking local SQLite. Sous WSL avec repo sur `/mnt/...`, le backend bascule automatiquement vers `~/.weather-mlops/mlflow`. |
| **FastAPI** | Endpoints JSON/CSV + métriques Prometheus. Port 8003. |
| **Streamlit** | Dashboard local connecté à l'API. |
| **Airflow** | Orchestration : ingestion quotidienne, réentraînement hebdomadaire, monitoring, backfill. |
| **Prometheus/Grafana** | Monitoring API via Docker Compose. |

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

> **Pourquoi pas NASA POWER ?** NASA POWER utilise ERA5 brut à 50 km (vs ERA5-Land 9 km pour Open-Meteo). Il renvoie la pression de surface (non corrigée MSL), pas de données horaires, et des rafales sous-estimées de 10-28 km/h. Incompatible avec ce projet station-to-station.

---

## Données

### Couverture

- **Période** : 2008-01-01 → hier (J-1)
- **Villes** : 26 villes australiennes
- **Lignes** : ~173 800 (26 × ~6 686 jours)
- **Colonnes brutes** : 27 (météo) + 8 (prédictions) = 35 au total dans `weather_final.csv`

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
| `weather_type_tomorrow` | Multi-classe | XGBoost multiclass | `Sunny`, `Cloudy`, `Rainy` |
| `comfort_score` | Score 0-100 | Formule | Température 18-24°C idéale, pénalités humidité/vent/pluie |
| `heatwave_risk` | Probabilité 0-1 | XGBoost classification | Risque canicule (≥35°C deux jours consécutifs) |
| `frost_risk` | Probabilité 0-1 | XGBoost classification | Risque gel (min_temp ≤ 2°C) |
| `storm_probability` | Probabilité 0-1 | XGBoost classification | Probabilité d'orage (codes WMO 95-99) |

### Métriques des modèles (entraînement sur 173 800 lignes, 2008-2026)

| Modèle | Accuracy / MAE | F1 / R² | AUC-ROC | Notes |
|---|---|---|---|---|
| `rain_tomorrow` | 78.4% | 0.677 | **0.868** | 29.1% de jours de pluie |
| `max_temp_tomorrow` | MAE 1.60°C | R² **0.912** | — | Erreur moyenne ±2°C |
| `weather_type_tomorrow` | 99.99% | F1_macro 0.556 | — | Classes déséquilibrées |
| `heatwave_risk` | 97.4% | 0.772 | **0.996** | 4.5% positifs |
| `frost_risk` | 94.0% | 0.506 | **0.988** | 3.1% positifs |
| `storm_probability` | 100% | 0.0 | — | Aucun orage en test (événement très rare) |

> **Note storm_probability :** AUC non calculable car 0% de positifs dans le jeu de test. Le modèle nécessite soit plus de données (étendre la période), soit une redéfinition du label storm (codes WMO plus larges).

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
- **Labels cibles** (décalés J+1) : `rain_tomorrow`, `max_temp_tomorrow`, `heatwave_risk`, `frost_risk`, `storm_label`

---

## Structure du projet

```text
weather-mlops/
├── api/
│   └── app.py                    # FastAPI — 9 endpoints + Prometheus
├── dags/
│   ├── ingestion_dag.py          # Ingestion quotidienne (06:00 UTC)
│   ├── train_dag.py              # Réentraînement hebdomadaire (lundi 02:00)
│   ├── monitoring_dag.py         # Qualité, couverture, drift, métriques
│   └── backfill_dag.py           # Backfill manuel paramétrable
├── pipeline/
│   ├── locations.py              # 26 villes (lat, lon, timezone, state)
│   ├── fetch_weather.py          # Appels Open-Meteo (archive + forecast)
│   ├── database.py               # Schéma SQLite, upserts, vue v_weather_full
│   ├── process_weather.py        # Feature engineering + labels
│   ├── train_models.py           # Entraînement XGBoost + MLflow
│   ├── predict.py                # Génération des prédictions
│   └── run_pipeline.py           # CLI d'orchestration
├── streamlit_app/
│   ├── app.py                    # Dashboard Streamlit
│   └── requirements.txt
├── tests/
│   ├── test_preprocess.py
│   ├── test_xgboost_model.py
│   ├── test_mlflow_config.py
│   ├── test_monitoring_branch.py
│   ├── test_ingestion_backfill_flow.py
│   └── test_train_dag.py
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   ├── dashboards/
│   └── provisioning/
├── data/
│   ├── weather.db                # SQLite (weather_raw + weather_predictions)
│   └── output/
│       └── weather_final.csv     # Vue complète exportée (~174 000 lignes)
├── models/                       # Modèles .pkl + metrics.json + feature importances
├── mlflow/                       # Tracking MLflow local (Windows / Docker)
├── config/
│   ├── mlops.yaml               # Seuils MLOps, gating, cooldowns
│   ├── modeling.yaml            # Hyperparamètres XGBoost, features, labels
│   └── settings.py              # Chargement typé des configs
├── docker-compose.yaml           # API + Prometheus + Grafana
├── Dockerfile                    # Image Python 3.11 slim pour l'API
└── requirements.txt
```

---

## Installation locale

**Prérequis :**
- Python 3.10+
- Git
- Docker Desktop (optionnel, pour Prometheus/Grafana)
- Apache Airflow (optionnel, pour l'orchestration planifiée)

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
| `repair --start-date ... --end-date ...` | Répare explicitement les dates manquantes ou incomplètes sur un intervalle |
| `export` | Export de `v_weather_full` vers `data/output/weather_final.csv` |

```bash
# Premier lancement complet (~4 min, 26 villes, 18 ans)
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
- **Résumé** : le backfill complet prend ~4-5 minutes pour 26 villes

En cas d'échec partiel (certaines villes en erreur), relancer sans `--force` pour ne re-télécharger que les villes manquantes :

```bash
python pipeline/run_pipeline.py backfill
```

Le backfill relance aussi une phase `repair_gaps` qui scanne l'intervalle demandé et rejoue les dates manquantes ou incomplètes avant le retrain.

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

### Table `weather_predictions`

Prédictions générées par les 6 modèles, une ligne par (date, ville).

### Vue `v_weather_full`

`LEFT JOIN weather_raw + weather_predictions` — table unifiée pour l'API et l'export CSV.

---

## API FastAPI

```bash
python api/app.py
```

URL locale : `http://localhost:8083`

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
curl "http://localhost:8083/api/weather/latest?city=Sydney"
```

---

## Dashboard Streamlit

Le dashboard consomme l'API FastAPI sur `http://localhost:8083`.
Démarrer l'API en premier, puis :

```bash
streamlit run streamlit_app/app.py
```

Affiche : dernières prédictions par ville, tendances historiques, métriques du dernier entraînement.

---

## Power BI

**Option recommandée — connecteur Web :**

1. Ouvrir Power BI Desktop
2. **Obtenir les données > Web**
3. URL : `http://localhost:8083/api/weather` ou `.../api/weather/latest`
4. Dans Power Query, développer le champ `data`

**Option CSV :**

```text
data/output/weather_final.csv
```

Régénéré automatiquement par `backfill`, `daily`, `train` et `export`.

---

## MLflow

```bash
mlflow ui --backend-store-uri sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db --port 5000
```

URL locale : `http://localhost:5000`
Sous Windows natif ou Docker, garde `sqlite:///mlflow/mlflow.db`.

Chaque entraînement crée un run parent (stats dataset) avec des runs enfants par modèle (params, métriques, artefacts).
L'expérience par défaut est `weather_australia` (définie dans `config/modeling.yaml`).

---

## Airflow

Les DAGs sont dans `dags/`.

| DAG | Schedule | Tâches |
|---|---|---|
| `weather_daily_ingestion` | `0 6 * * *` | init DB → ingestion J-1 → prédictions → export |
| `weather_weekly_train` | `0 2 * * 1` | réentraînement → prédictions → export |
| `weather_daily_monitoring` | `0 8 * * *` | qualité, couverture, drift, métriques, alertes |
| `weather_backfill` | Manuel | backfill paramétrable → entraînement → prédictions → export |

```bash
# Installation (depuis le home Linux pour éviter les problèmes WSL/NTFS)
cd ~ && pip install apache-airflow --no-cache-dir

# Lancement tout-en-un (webserver + scheduler + DB init automatique)
# Port 8083 réservé à Airflow ; FastAPI tourne sur 8003
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8083 airflow standalone
```

L'UI est accessible sur `localhost:8083`. Le mot de passe admin est affiché au premier démarrage dans les logs (`standalone | Login with username: admin  password: ...`).

---

## Monitoring Docker

```bash
docker compose up --build
```

| Service | URL | Notes |
|---|---|---|
| API FastAPI | http://localhost:8083 | |
| Prometheus | http://localhost:9090 | Scrape `/metrics` toutes les 15 s |
| Grafana | http://localhost:3000 | Identifiants : admin / admin |

---

## Tests

```bash
pytest tests
```

Couvrent le feature engineering, les helpers de training et la persistance des modèles.
Couvrent aussi la config MLflow, la logique de branching/gating des DAGs Airflow, l'idempotence du daily ingestion et la reprise incrémentale du backfill.

---

## Fichiers générés (non versionnés)

```text
data/weather.db
data/output/weather_final.csv
data/monitoring/*.json
models/*.pkl
models/metrics.json
mlflow/mlflow.db
~/.weather-mlops/mlflow/mlflow.db   # créé automatiquement sous WSL sur /mnt/...
logs/pipeline.log
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
