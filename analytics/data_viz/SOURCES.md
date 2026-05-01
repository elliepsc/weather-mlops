# Sources Power BI — weather-mlops

Les marts sont exportés automatiquement par le DAG `weather_dbt_analytics`
(tâche `git_push_analytics`) et disponibles en 3 modes selon le paramètre
`DataMode` dans `template.pq`.

---

## URLs GitHub raw (DataMode = "github")

```
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_forecast_vs_actual_timeline.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_overview.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_by_city.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_mlops_health.csv
https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_retraining_history.csv
```

---

## Chemins locaux (DataMode = "local")

```
C:/Users/<user>/weather-mlops/data/analytics/mart_forecast_vs_actual_timeline.csv
C:/Users/<user>/weather-mlops/data/analytics/mart_model_performance_overview.csv
C:/Users/<user>/weather-mlops/data/analytics/mart_model_performance_by_city.csv
C:/Users/<user>/weather-mlops/data/analytics/mart_mlops_health.csv
C:/Users/<user>/weather-mlops/data/analytics/mart_retraining_history.csv
```

Définir `LocalBasePath` comme paramètre PBI, ex. `C:/Users/Ellie Pro/weather-mlops/data/analytics/`.

---

## Tables BigQuery (DataMode = "bigquery") — Phase 2 GCP

| Mart | Table BigQuery |
|---|---|
| mart_forecast_vs_actual_timeline | `<BQProject>.<BQDataset>.mart_forecast_vs_actual_timeline` |
| mart_model_performance_overview | `<BQProject>.<BQDataset>.mart_model_performance_overview` |
| mart_model_performance_by_city | `<BQProject>.<BQDataset>.mart_model_performance_by_city` |
| mart_mlops_health | `<BQProject>.<BQDataset>.mart_mlops_health` |
| mart_retraining_history | `<BQProject>.<BQDataset>.mart_retraining_history` |

---

## Changer DataMode dans Power BI Desktop

1. **Accueil → Transformer les données** (ouvre Power Query Editor)
2. Dans le volet gauche, cliquer sur **DataMode**
3. Modifier la valeur dans la barre de formule (`"github"`, `"local"` ou `"bigquery"`)
4. Si nécessaire, mettre à jour **LocalBasePath**, **BQProject**, **BQDataset** de la même façon
5. **Fermer & Appliquer** → actualiser les données

---

## Créer les paramètres dans Power BI Desktop (première fois)

1. **Accueil → Gérer les paramètres → Nouveau paramètre**
2. Créer chaque paramètre :

| Nom | Type | Valeur par défaut |
|---|---|---|
| `DataMode` | Texte | `github` |
| `LocalBasePath` | Texte | `C:/Users/xxx/weather-mlops/data/analytics/` |
| `BQProject` | Texte | `your-gcp-project` |
| `BQDataset` | Texte | `weather_mlops` |

3. Coller les requêtes de `template.pq` dans des nouvelles requêtes vides
4. La requête `GetSource` doit avoir **"Activer le chargement" désactivé**

---

## Schéma des marts

### mart_forecast_vs_actual_timeline
Grain : ville × jour (90 derniers jours)

| Colonne | Type PBI | Description |
|---|---|---|
| `prediction_date` | Date | Date de la prédiction (J) |
| `city` | Text | Nom de la ville |
| `state` | Text | État australien |
| `pred_rain_tomorrow` | True/False | Pluie prévue J+1 |
| `pred_rain_proba` | Decimal Number | Probabilité pluie J+1 (0–1) |
| `pred_max_temp_tomorrow` | Decimal Number | Température max prévue J+1 (°C) |
| `pred_weather_type_tomorrow` | Text | Sunny / Cloudy / Rainy / Stormy |
| `pred_heatwave_risk` | True/False | Risque canicule J+1 |
| `pred_frost_risk` | True/False | Risque gel J+1 |
| `pred_storm_probability` | Decimal Number | Probabilité storm (0–1) |
| `comfort_score` | Decimal Number | Score confort 0–100 |
| `actual_date` | Date | Date des actuals (J+1) |
| `actual_rain` | True/False | Pluie réelle J+1 |
| `actual_max_temp` | Decimal Number | Température max réelle J+1 (°C) |
| `actual_min_temp` | Decimal Number | Température min réelle J+1 (°C) |
| `actual_rainfall` | Decimal Number | Précipitations réelles J+1 (mm) |
| `rain_correct` | True/False | Prédiction pluie correcte |
| `temp_abs_error` | Decimal Number | Erreur absolue température (°C) |
| `has_actuals` | True/False | Actuals disponibles pour ce jour |
| `rain_accuracy_30d` | Decimal Number | Accuracy pluie rolling 30j |
| `temp_mae_30d` | Decimal Number | MAE temp rolling 30j (°C) |
| `rain_accuracy_7d` | Decimal Number | Accuracy pluie rolling 7j |
| `temp_mae_7d` | Decimal Number | MAE temp rolling 7j (°C) |

---

### mart_model_performance_overview
Grain : mois

| Colonne | Type PBI | Description |
|---|---|---|
| `month` | Date | Premier jour du mois (YYYY-MM-01) |
| `n_city_days` | Whole Number | Nombre de ville×jour évalués |
| `n_days` | Whole Number | Nombre de jours distincts |
| `n_cities` | Whole Number | Nombre de villes distinctes |
| `rain_accuracy` | Decimal Number | Accuracy pluie (0–1) |
| `temp_mae` | Decimal Number | MAE température (°C) |
| `temp_max_error` | Decimal Number | Erreur max température (°C) |
| `temp_min_error` | Decimal Number | Erreur min température (°C) |
| `pct_with_actuals` | Decimal Number | % ville×jours avec actuals (0–100) |

---

### mart_model_performance_by_city
Grain : ville × mois

| Colonne | Type PBI | Description |
|---|---|---|
| `month` | Date | Premier jour du mois (YYYY-MM-01) |
| `city` | Text | Nom de la ville |
| `n_days` | Whole Number | Nombre de jours évalués |
| `rain_accuracy` | Decimal Number | Accuracy pluie (0–1) |
| `temp_mae` | Decimal Number | MAE température (°C) |
| `temp_max_error` | Decimal Number | Erreur max température (°C) |
| `rain_accuracy_30d_eom` | Decimal Number | Accuracy 30j fin de mois |
| `temp_mae_30d_eom` | Decimal Number | MAE 30j fin de mois (°C) |
| `state` | Text | État australien |
| `latitude` | Decimal Number | Latitude de la ville |
| `longitude` | Decimal Number | Longitude de la ville |

---

### mart_mlops_health
Grain : jour (30 derniers jours)

| Colonne | Type PBI | Description |
|---|---|---|
| `date` | Date | Date |
| `completeness_pct` | Decimal Number | % villes ingérées (0–100) |
| `missing_city_count` | Whole Number | Nombre de villes manquantes |
| `present_cities` | Whole Number | Nombre de villes présentes |
| `expected_cities` | Whole Number | Nombre de villes attendues (26) |
| `has_gap` | True/False | Gap d'ingestion ce jour |
| `monitoring_action` | Text | trigger_retrain / alert_only / alert_insufficient_data / no_action |
| `monitoring_reason` | Text | Raison de la décision monitoring |
| `n_drifted_features` | Whole Number | Nombre de features en drift |
| `heavy_drift` | True/False | Drift sévère détecté |
| `mild_drift` | True/False | Drift léger détecté |
| `low_accuracy` | True/False | Accuracy sous seuil |
| `high_mae` | True/False | MAE au-dessus du seuil |
| `rain_accuracy_30d` | Decimal Number | Accuracy pluie rolling 30j |
| `temp_mae_30d` | Decimal Number | MAE temp rolling 30j (°C) |
| `last_retrain_date` | Date | Date du dernier retrain |
| `days_since_retrain` | Whole Number | Jours depuis le dernier retrain |

---

### mart_retraining_history
Grain : événement de retrain

| Colonne | Type PBI | Description |
|---|---|---|
| `retrain_date` | Date | Date du retrain |
| `source` | Text | Source de l'événement |
| `trigger_action` | Text | Action déclenchée |
| `trigger_reason` | Text | Raison du déclenchement |
| `n_drifted_features` | Whole Number | Features en drift au moment du retrain |
| `heavy_drift` | True/False | Drift sévère présent |
| `low_accuracy` | True/False | Accuracy sous seuil |
| `preceded_by_gap` | True/False | Gap de données dans les 7j précédents |
| `gap_days_in_7d_window` | Whole Number | Jours de gap sur la fenêtre 7j |
| `max_missing_cities_7d` | Whole Number | Max villes manquantes sur 7j |
| `avg_completeness_7d_pct` | Decimal Number | Complétude moyenne 7j (%) |
| `rain_accuracy_at_retrain` | Decimal Number | Accuracy pluie au moment du retrain |
| `temp_mae_at_retrain` | Decimal Number | MAE temp au moment du retrain (°C) |
| `next_retrain_date` | Date | Date du retrain suivant (nullable) |
| `next_rain_accuracy` | Decimal Number | Accuracy après retrain (nullable) |
| `next_temp_mae` | Decimal Number | MAE après retrain (nullable) |
| `accuracy_delta` | Decimal Number | Δ accuracy (positif = amélioration) |
| `mae_delta` | Decimal Number | Δ MAE (positif = amélioration) |
| `accuracy_degraded_after_retrain` | True/False | Dégradation post-retrain |
