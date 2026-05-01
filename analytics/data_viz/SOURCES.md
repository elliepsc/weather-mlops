# Sources Power BI — weather-mlops

Les marts sont exportés automatiquement par le DAG `weather_dbt_analytics`
(tâche `git_push_analytics`) et disponibles en lecture directe via GitHub raw.

---

## URLs raw GitHub

```
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_overview.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_by_city.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_forecast_vs_actual_timeline.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_mlops_health.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_retraining_history.csv
```

---

## Schéma des marts

### mart_model_performance_overview

Grain : mois. Métriques globales de tous les modèles.

| Colonne | Type PBI | Description |
|---|---|---|
| `month` | Date | Premier jour du mois (YYYY-MM-01) |
| `rain_accuracy` | Decimal Number | Accuracy prédiction pluie J+1 (0–1) |
| `rain_auc` | Decimal Number | AUC ROC pluie J+1 |
| `max_temp_mae` | Decimal Number | MAE température max J+1 (°C) |
| `max_temp_r2` | Decimal Number | R² température max J+1 |
| `weather_type_accuracy` | Decimal Number | Accuracy type météo J+1 (0–1) |
| `heatwave_auc` | Decimal Number | AUC ROC risque canicule |
| `frost_auc` | Decimal Number | AUC ROC risque gel |
| `storm_auc` | Decimal Number | AUC ROC probabilité storm |
| `row_count` | Whole Number | Nombre de lignes évaluées |

---

### mart_model_performance_by_city

Grain : ville × mois. Dégradation des métriques par ville.

| Colonne | Type PBI | Description |
|---|---|---|
| `month` | Date | Premier jour du mois (YYYY-MM-01) |
| `city` | Text | Nom de la ville |
| `state` | Text | État australien |
| `rain_accuracy` | Decimal Number | Accuracy pluie J+1 pour cette ville |
| `rain_auc` | Decimal Number | AUC ROC pluie J+1 |
| `max_temp_mae` | Decimal Number | MAE température max J+1 (°C) |
| `weather_type_accuracy` | Decimal Number | Accuracy type météo |
| `row_count` | Whole Number | Nombre de jours évalués |

---

### mart_forecast_vs_actual_timeline

Grain : ville × jour (90 derniers jours). Prédictions vs réel alignés J+1.

| Colonne | Type PBI | Description |
|---|---|---|
| `date` | Date | Date de la prédiction (J) |
| `city` | Text | Nom de la ville |
| `predicted_rain_tomorrow` | True/False | Pluie prévue J+1 |
| `actual_rain_tomorrow` | True/False | Pluie réelle J+1 |
| `rain_correct` | True/False | Prédiction correcte |
| `predicted_max_temp` | Decimal Number | Température max prévue J+1 (°C) |
| `actual_max_temp` | Decimal Number | Température max réelle J+1 (°C) |
| `temp_error` | Decimal Number | Erreur absolue température (°C) |
| `predicted_weather_type` | Text | Type météo prévu (Sunny/Cloudy/Rainy/Stormy) |
| `actual_weather_type` | Text | Type météo réel |
| `comfort_score` | Decimal Number | Score de confort 0–100 |
| `heatwave_risk` | True/False | Risque canicule prévu |
| `frost_risk` | True/False | Risque gel prévu |

---

### mart_mlops_health

Grain : jour (30 derniers jours). Complétude ingestion et décisions MLOps.

| Colonne | Type PBI | Description |
|---|---|---|
| `date` | Date | Date de la journée |
| `ingestion_completeness` | Decimal Number | % villes ingérées (0–1) |
| `cities_ok` | Whole Number | Nombre de villes avec données complètes |
| `cities_total` | Whole Number | Nombre total de villes (26) |
| `monitoring_decision` | Text | trigger_retrain / alert_only / alert_insufficient_data / no_action |
| `drift_detected` | True/False | Drift détecté par le monitoring |
| `drift_score` | Decimal Number | Score de drift (0–1) |
| `data_sufficient` | True/False | Données suffisantes pour monitoring |
| `cooldown_active` | True/False | Cooldown retrain actif |

---

### mart_retraining_history

Grain : événement de retrain. Audit avant/après chaque réentraînement.

| Colonne | Type PBI | Description |
|---|---|---|
| `retrain_date` | Date | Date du retrain |
| `trigger_reason` | Text | Raison du déclenchement |
| `rain_auc_before` | Decimal Number | AUC pluie avant retrain |
| `rain_auc_after` | Decimal Number | AUC pluie après retrain |
| `max_temp_mae_before` | Decimal Number | MAE temp avant retrain (°C) |
| `max_temp_mae_after` | Decimal Number | MAE temp après retrain (°C) |
| `model_promoted` | True/False | Nouveau modèle promu (vs rollback) |
| `rollback` | True/False | Rollback vers baseline déclenché |
| `training_rows` | Whole Number | Lignes utilisées pour l'entraînement |

---

## Phase 2 — Migration BigQuery

Quand les données seront dans BigQuery, remplacer les sources GitHub raw
par le connecteur natif Power BI.

**Connecteur à utiliser :** Google BigQuery  
Power BI Desktop → Obtenir les données → Google BigQuery

**Dans `definition.pbidataset` :** remplacer chaque requête `Web.Contents`
par `GoogleBigQuery.Database("project-id", "dataset_id")` puis sélectionner
la table correspondante (`mart_model_performance_overview`, etc.).

**Champs à mettre à jour dans chaque requête Power Query M :**
```m
// Avant (Phase 1)
let
    Source = Web.Contents("https://raw.githubusercontent.com/..."),
    ...

// Après (Phase 2)
let
    Source = GoogleBigQuery.Database("mon-projet-gcp", "analytics"),
    Table = Source{[Schema="analytics", Item="mart_model_performance_overview"]}[Data],
    ...
```

Les types de colonnes définis dans `template.pq` restent identiques —
seule la source change.
