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
| Monitoring qualité / drift | ✗ | ✅ `weather_daily_monitoring` (automatique 8h) |
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
API_PORT=8083
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=weather
```

> Open-Meteo est gratuit et sans clé API. Aucune variable secrète requise pour le fonctionnement de base.

---

## PARTIE 2 — Premier lancement (backfill)

### 2.1 Via CLI (recommandé pour la première fois)

```bash
python pipeline/run_pipeline.py backfill
```

Étapes exécutées dans l'ordre :
1. `init_db` — crée `data/weather.db` + tables + vue `v_weather_full`
2. Télécharge 2008-01-01 → J-1 pour les 26 villes (Open-Meteo, 10 s entre villes)
3. Entraîne les 6 modèles XGBoost (~10 min sur 173 k lignes)
4. Génère ~173 k prédictions
5. Exporte `data/output/weather_final.csv`

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
  "end_date":   "2026-04-21"
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

## PARTIE 3 — Démarrer les services

### 3.1 API FastAPI (obligatoire pour Streamlit et Power BI)

```bash
python api/app.py
# → http://localhost:8083
# → http://localhost:8083/docs  (Swagger)
```

Vérification :
```bash
curl http://localhost:8083/health
# Attendu : {"status":"ok","db":true}

curl "http://localhost:8083/api/weather/latest?city=Sydney"
```

### 3.2 Dashboard Streamlit

```bash
# Terminal séparé (API doit tourner)
streamlit run streamlit_app/app.py
# → http://localhost:8501
```

### 3.3 MLflow UI

```bash
mlflow ui --backend-store-uri sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db --port 5000
# → http://localhost:5000
# Expérience : weather_australia
```

Sous Windows natif ou Docker, utilise `sqlite:///mlflow/mlflow.db`.

### 3.4 Docker Compose — tous les services (API + Prometheus + Grafana + Airflow)

#### Premier lancement

```bash
docker compose up -d
```

`-d` = détaché, sinon le terminal est bloqué indéfiniment.  
Ne pas utiliser `--build` sauf si les Dockerfiles ont changé.

| Service | URL | Identifiants |
|---|---|---|
| API FastAPI | http://localhost:8003 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Airflow UI | http://localhost:8083 | voir ci-dessous |

#### Identifiants Airflow (SimpleAuthManager — Airflow 3.x)

Au **premier démarrage**, Airflow génère un mot de passe aléatoire et l'affiche dans les logs :

```bash
docker compose logs airflow-webserver | grep "Password for user"
# Simple auth manager | Password for user 'admin': <mot_de_passe_généré>
```

Pour fixer un mot de passe permanent, ajouter dans `airflow-webserver` **et** `airflow-scheduler` du `docker-compose.yaml` :

```yaml
environment:
  AIRFLOW__SIMPLE_AUTH_MANAGER__PASSWORDS: "admin:mon_mot_de_passe"
```

Puis `docker compose up -d --force-recreate airflow-webserver airflow-scheduler`.

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
| `airflow users create` | idem, mais `--role Admin` → SimpleAuthManager |

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

### 4.1 Installation Airflow

```bash
pip install apache-airflow
```

### 4.2 Initialisation (une seule fois)

```bash
airflow db init

airflow users create \
  --username admin \
  --password weather \
  --role Admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@example.com
```

### 4.3 Démarrer Airflow

```bash
# Terminal 1
airflow webserver --port 8081   # port 8081 pour éviter conflit avec l'API FastAPI

# Terminal 2
airflow scheduler
```

UI : http://localhost:8081 — identifiants : `admin / weather`

### 4.3 bis — Redemarrer Airflow proprement via `start_airflow.sh`

Si tu veux lancer Airflow depuis `~`, crée un symlink vers le script du repo. Comme ça,
les modifications de `start_airflow.sh` sont prises en compte sans recopier le fichier.

```bash
# Arrete Airflow
pkill -f "airflow standalone"

# Une seule fois, depuis le repo
cd /chemin/vers/weather-mlops
ln -sfn "$(pwd)/start_airflow.sh" ~/start_airflow.sh

# Relance
bash ~/start_airflow.sh
```

### 4.4 DAGs disponibles

| DAG | Schedule | Déclenchement | Tâches |
|---|---|---|---|
| `weather_daily_ingestion` | `0 6 * * *` | Automatique | init_db → fetch J-1 → predict → export |
| `weather_weekly_train` | `0 2 * * 1` | Automatique (lundi) | retrain → predict → export |
| `weather_daily_monitoring` | `0 8 * * *` | Automatique | qualité → couverture → drift → métriques → retrain auto si dégradation |
| `weather_backfill` | Manuel | Trigger UI avec config JSON | fetch historique → retrain → predict → export |

### 4.5 Activer les DAGs

Dans l'UI Airflow (http://localhost:8081) :
1. Cliquer sur le toggle à gauche de chaque DAG pour l'activer
2. Pour `weather_backfill` : cliquer **Trigger DAG ▶** → fournir le JSON de config si nécessaire

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

### 6.2 Manuel

```bash
python pipeline/run_pipeline.py train
```

Durée : ~10 minutes sur 173 k lignes.

### 6.3 Comparer les métriques avant/après

```bash
# Voir les runs MLflow
python -c "
import mlflow
mlflow.set_tracking_uri('sqlite:////home/$USER/.weather-mlops/mlflow/mlflow.db')
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
            print(f'  {k}: {round(v,4) if v else v}')
        print()
"
```

Métriques de référence (entraînement sur 173 k lignes, 2008-2026) :

| Modèle | Accuracy / MAE | F1 / R² | AUC | Seuil d'alerte |
|---|---|---|---|---|
| `rain_tomorrow` | 77.4% | 0.651 | 0.856 | AUC < 0.80 |
| `max_temp_tomorrow` | MAE 1.63°C | R² 0.908 | — | R² < 0.85 |
| `weather_type_tomorrow` | 88.5% | F1_macro 0.833 | — | acc < 0.80 |
| `heatwave_risk` | 97.2% | 0.752 | 0.996 | AUC < 0.95 |
| `frost_risk` | 94.3% | 0.474 | 0.988 | AUC < 0.95 |
| `storm_probability` | — | 0.0 | ⚠️ N/A | Problème connu |

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

Le DAG `weather_daily_monitoring` (08:00 UTC) effectue chaque jour :

1. **Qualité des données** — vérifie que ≥ 80 % des 26 villes ont leurs données J-1
2. **Couverture des prédictions** — signale les villes dont les prédictions ne sont pas à jour
3. **Détection de drift** — test KS entre les 30 derniers jours et les 30 jours précédents
4. **Métriques modèle** — accuracy pluie et MAE température sur 30 jours glissants
5. **Retrain automatique** si l'une des conditions est vraie :

| Condition | Seuil |
|---|---|
| Drift KS détecté | p < 0.05 sur ≥ 1 feature |
| Accuracy pluie | < 75% sur 30 jours |

En cas de déclenchement, `weather_weekly_train` est lancé automatiquement sans intervention manuelle.

**Lire les résultats :**
```bash
cat ~/weather-mlops/data/monitoring/model_metrics.json   # accuracy + MAE
cat ~/weather-mlops/data/monitoring/drift_report.json    # KS stat par feature
```

**Lancement manuel :**
```bash
cd ~ && airflow dags trigger weather_daily_monitoring
```

**Dans l'UI :** tâche `trigger_retrain` verte = retrain lancé / `no_action` verte = modèle stable.

### 7.4 Note — storm_probability

Ce modèle retourne AUC=0 / F1=0 car les codes WMO 95-99 (orage) sont quasi absents dans ERA5-Land. Ce n'est pas un bug de code mais une limitation de la source de données. Options :
- Élargir le label storm aux codes WMO 80-84 (averses fortes) dans `process_weather.py`
- Accepter la limitation et ignorer cette prédiction dans les analyses

---

## PARTIE 8 — Power BI

### Option 1 — Connecteur Web (données en temps réel)

1. Ouvrir Power BI Desktop
2. **Obtenir les données > Web**
3. URL : `http://localhost:8083/api/weather`
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
pytest tests/ -v
```

59 tests couvrant :
- Feature engineering (`test_preprocess.py`)
- Mapping enrichi Open-Meteo (`test_fetch_weather.py`)
- Entraînement et persistance modèles (`test_xgboost_model.py`)
- Configuration MLflow (`test_mlflow_config.py`)
- Configuration modèle (`test_modeling_config.py`)
- Branching monitoring (`test_monitoring_branch.py`)
- Gate et rollback du train DAG (`test_train_dag.py`)
- Idempotence du daily ingestion et reprise incrémentale du backfill (`test_ingestion_backfill_flow.py`)

Résultat attendu : `59 passed`

---

## PARTIE 10 — Procédures de récupération

### 10.1 Villes manquantes (après rate limit)

```bash
# Identifier les villes avec données NASA POWER incorrectes (pression < 990 hPa)
python -c "
import sqlite3
conn = sqlite3.connect('data/weather.db')
rows = conn.execute('''
    SELECT city, ROUND(AVG(pressure_9am),1) as avg_p
    FROM weather_raw
    GROUP BY city
    HAVING avg_p < 990
    ORDER BY avg_p
''').fetchall()
for r in rows:
    print(f'  {r[0]:<15} {r[1]} hPa  <- données NASA POWER incorrectes')
conn.close()
"

# Re-télécharger uniquement les villes manquantes (sans --force)
python pipeline/run_pipeline.py backfill
```

### 10.2 Reconstruire la base de zéro

```bash
rm data/weather.db
python pipeline/run_pipeline.py backfill
```

### 10.3 Régénérer les prédictions sans réentraîner

```bash
python pipeline/run_pipeline.py predict
```

### 10.4 Régénérer le CSV uniquement

```bash
python pipeline/run_pipeline.py export
```

### 10.5 Forcer le re-téléchargement de toutes les villes

```bash
python pipeline/run_pipeline.py backfill --force
# Attention : efface et re-télécharge toutes les données (~15 min)
```

---

## PARTIE 11 — Checklist de vérification complète

```bash
# 1. Base de données
python -c "
import sqlite3
conn = sqlite3.connect('data/weather.db')
r = conn.execute('SELECT COUNT(*), COUNT(DISTINCT city), MIN(date), MAX(date) FROM weather_raw').fetchone()
p = conn.execute('SELECT COUNT(*), COUNT(DISTINCT city) FROM weather_predictions').fetchone()
bad_press = conn.execute('SELECT COUNT(*) FROM weather_raw WHERE pressure_9am < 990').fetchone()[0]
print('DB brutes  :', r[0], 'lignes |', r[2], '->', r[3], '| villes:', r[1])
print('Prédictions:', p[0], 'lignes | villes:', p[1])
print('Pression incorrecte (NASA):', bad_press, 'lignes')
conn.close()
"

# 2. Modèles
python -c "
import os, json
pkls = [f for f in os.listdir('models') if f.endswith('.pkl') and 'encoder' not in f and 'mappings' not in f]
print('Modèles :', len(pkls), '/ 6')
with open('models/metrics.json') as f:
    m = json.load(f)
print('rain AUC  :', m['rain_tomorrow']['auc_roc'])
print('temp R²   :', m['max_temp_tomorrow']['r2'])
"

# 3. CSV
python -c "
rows = sum(1 for _ in open('data/output/weather_final.csv')) - 1
print('CSV       :', rows, 'lignes')
"

# 4. API
curl -s http://localhost:8083/health

# 5. Tests
pytest tests/ -q
```

---

## Schéma de flux complet

```
INSTALLATION (une fois)
    git clone → venv → pip install → cp .env
          |
          v
BACKFILL (une fois, CLI ou DAG manuel)
    fetch 2008-2026 → train 6 modèles → predict → export CSV
          |
          v
SERVICES (démarrer manuellement ou via gestionnaire de processus)
    python api/app.py          → :8083  (FastAPI)
    streamlit run app.py       → :8501  (Dashboard)
    mlflow ui                  → :5000  (Tracking)
    docker compose up          → :9090/:3000 (Prometheus/Grafana)
    airflow webserver+scheduler→ :8081  (Orchestrateur)
          |
          v
CYCLE QUOTIDIEN (Airflow automatique)
    06:00 UTC — weather_daily_ingestion
        fetch J-1 → predict → export
    08:00 UTC — weather_daily_monitoring
        qualité → drift → métriques → alerte si problème
          |
          v
CYCLE HEBDOMADAIRE (Airflow automatique)
    Lundi 02:00 UTC — weather_weekly_train
        retrain 6 modèles → predict → export → MLflow log
          |
          v
CONSOMMATION
    Power BI  → http://localhost:8083/api/weather
    Streamlit → http://localhost:8501
    CSV       → data/output/weather_final.csv
    MLflow    → http://localhost:5000
```
