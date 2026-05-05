# GCP Migration Guide — Phase 2

Migration de la destination des exports analytics de CSV/GitHub vers **GCS + BigQuery External Tables**.

---

## 1. Prérequis

| Élément | Détail |
|---|---|
| Compte GCP | Projet existant avec facturation activée |
| Google Cloud SDK | `gcloud` installé et authentifié (`gcloud auth login`) |
| Service Account | Rôles : `Storage Object Admin`, `BigQuery Data Editor`, `BigQuery Job User` |
| Python packages | `pip install google-cloud-storage google-cloud-bigquery dbt-bigquery` |
| Fichier de clé SA | JSON exporté depuis GCP Console (CI/CD uniquement — local utilise oauth) |

---

## 2. Étapes dans l'ordre

### a. Créer le bucket GCS

```bash
gcloud storage buckets create gs://MY_BUCKET \
  --location=EU \
  --uniform-bucket-level-access
```

Remplacer `MY_BUCKET` par la valeur qui sera mise dans `GCS_BUCKET`.

### b. Configurer `.env`

Copier `.env.example` vers `.env` et remplir :

```env
GCP_PROJECT_ID=my-gcp-project-123
GCP_DATASET=weather_mlops
GCP_LOCATION=EU
GCS_BUCKET=my-bucket-weather-mlops
GCS_ENABLED=true

# Local (oauth) — ne pas setter GOOGLE_APPLICATION_CREDENTIALS
# CI/CD (service-account)
DBT_BIGQUERY_METHOD=service-account
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Pour l'authentification locale oauth :

```bash
gcloud auth application-default login
```

### c. Tester l'export Parquet vers GCS

```bash
python pipeline/export_to_gcs.py
```

Vérifier dans les logs que les fichiers sont uploadés :

```
INFO  uploaded gs://my-bucket/weather-mlops/marts/mart_forecast_vs_actual_timeline/2024/01/data.parquet (42.3 KB)
INFO  GCS export done: 48 files, 3.2 MB, 12.4s
```

Structure dans GCS :

```
gs://MY_BUCKET/weather-mlops/marts/
  mart_forecast_vs_actual_timeline/
    2024/01/data.parquet        ← partitionné par prediction_date
    2024/02/data.parquet
    ...
  mart_model_performance_overview/
    mart_model_performance_overview.parquet   ← fichier unique
  ...
```

### d. Créer les tables externes BigQuery

```bash
python analytics/scripts/setup_bigquery.py
```

Ce script crée le dataset `weather_mlops` s'il n'existe pas, puis crée une
`EXTERNAL TABLE` par mart pointant vers les fichiers Parquet dans GCS.

Vérifier dans BigQuery Console :
`Projet → weather_mlops → Tables` — toutes les tables doivent apparaître.

### e. Tester dbt sur BigQuery

```bash
./analytics/scripts/run_dbt_bigquery.sh
```

Ou manuellement :

```bash
cd analytics/
dbt run  --target bigquery --profiles-dir . --project-dir .
dbt test --target bigquery --profiles-dir .
```

### f. Changer la source Power BI

**Avant (Phase 1)** : `Get Data → Text/CSV → data/analytics/*.csv`

**Après (Phase 2)** : `Get Data → BigQuery → Projet → weather_mlops → sélectionner les tables`

---

## 3. Ce qui NE change PAS

| Composant | Status |
|---|---|
| Airflow DAGs (ingestion, monitoring, train) | Inchangé |
| Pipeline ML (fetch, process, train, predict) | Inchangé |
| Streamlit dashboard | Inchangé |
| FastAPI (`api/app.py`) | Inchangé |
| Modèles ML (`models/`) | Inchangé |
| DuckDB local (`data/analytics.duckdb`) | Toujours produit, utilisé pour l'export |
| Export CSV (`data/analytics/*.csv`) | Toujours produit par `export_analytics_csv` |
| Profil dbt `duckdb` (cible par défaut) | Inchangé — `dbt run` sans `--target` utilise toujours DuckDB |

---

## 4. Ce qui CHANGE

| Composant | Avant | Après |
|---|---|---|
| Export GCS | Absent | `export_gcs_parquet` (tâche Airflow, opt-in via `GCS_ENABLED=true`) |
| Tables BigQuery | Absent | External Tables créées par `setup_bigquery.py` |
| Source Power BI | Fichiers CSV locaux / GitHub | BigQuery External Tables |
| Profil dbt actif | `duckdb` | `bigquery` (via `--target bigquery`) |

---

## 5. Notes sur la compatibilité SQL dbt

### Macro `datediff_days` — déjà cross-adapter

La macro `analytics/macros/datediff_days.sql` supporte DuckDB et BigQuery via
`adapter.dispatch()`. Aucune modification nécessaire.

### Modèles avec `DATEDIFF` natif à migrer

Les fichiers suivants utilisent `DATEDIFF('day', ...)` directement (syntaxe
DuckDB uniquement) et **casseront sur BigQuery** si `dbt run --target bigquery`
est exécuté sur les modèles dbt (pas les external tables) :

| Fichier | Occurrences | Correction à appliquer |
|---|---|---|
| [analytics/models/intermediate/int_data_freshness_by_city.sql](../analytics/models/intermediate/int_data_freshness_by_city.sql) | 6 | Remplacer `DATEDIFF('day', a, b)` par `{{ datediff_days('b', 'a') }}` |
| [analytics/models/marts/operations/mart_data_freshness_by_city.sql](../analytics/models/marts/operations/mart_data_freshness_by_city.sql) | 1 | Idem |

> **Phase 1** : ces modèles tournent en DuckDB — aucun impact immédiat.
> **Phase 2** : à corriger avant de lancer `dbt run --target bigquery` sur ces modèles.

Tous les autres modèles utilisent `{{ dbt.dateadd(...) }}` ou
`{{ datediff_days(...) }}` — cross-adapter natif.

---

## 6. CI/CD (GitHub Actions)

Ajouter les secrets dans le repo GitHub :

```
GCP_PROJECT_ID
GCS_BUCKET
GOOGLE_APPLICATION_CREDENTIALS  ← contenu JSON du service account (pas le chemin)
```

Dans la step qui lance le DAG Airflow, setter :

```yaml
env:
  DBT_BIGQUERY_METHOD: service-account
  GCS_ENABLED: "true"
  GCS_BUCKET: ${{ secrets.GCS_BUCKET }}
  GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
  GOOGLE_APPLICATION_CREDENTIALS: /tmp/sa_key.json
```
