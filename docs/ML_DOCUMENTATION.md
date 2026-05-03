# Documentation Technique ML — Weather MLOps

> **Audience cible :** recruteur data ou data scientist évaluant le portfolio.
> **Dernière mise à jour :** 2026-05-01 — métriques issues de `models/metrics.json`.

---

## 1. Problème et objectif

Ce projet prédit les conditions météorologiques du lendemain (horizon J+1) pour 26 villes australiennes, en s'appuyant sur les données quotidiennes de l'API Open-Meteo. Six grandeurs sont prédites : pluie (binaire), température maximale (régression), type météo (multi-classe), risque de canicule, risque de gel, et probabilité d'orage. Ce n'est pas un projet ML isolé mais un **système MLOps complet** : les distributions météorologiques changent significativement d'une saison à l'autre en Australie (été austral Dec–Fév vs. hiver Jun–Août, amplitude de 20°C sur certaines villes), ce qui rend le concept drift structurel et prévisible. Le pipeline répond à ce défi avec un monitoring quotidien automatisé par Airflow, un test de dérive statistique sur les features clés, et un mécanisme de retraining conditionnel avec validation contre une baseline — le tout tracé dans MLflow.

---

## 2. Variables prédites (targets)

| Modèle | Variable cible | Type | Horizon | Métrique principale |
|---|---|---|---|---|
| `rain_tomorrow` | Pluie J+1 (rainfall > 1 mm) | Classification binaire | 24 h | AUC-ROC |
| `max_temp_tomorrow` | Température max J+1 (°C) | Régression | 24 h | MAE (°C) |
| `weather_type_tomorrow` | Sunny / Cloudy / Rainy / Stormy | Classification multi-classe | 24 h | F1-macro |
| `heatwave_risk` | Risque de canicule J+1 | Classification binaire | 24 h | AUC-ROC |
| `frost_risk` | Risque de gel J+1 | Classification binaire | 24 h | AUC-ROC |
| `storm_probability` | Probabilité d'orage sévère J+1 | Classification binaire | 24 h | AUC-ROC |

> `comfort_score` (confort météo 0–100) est calculé par formule, sans modèle ML.

### Définition précise des labels

Les labels sont construits dans `pipeline/process_weather.py` par décalage temporel d'un jour (`shift(-1)`) **à l'intérieur de chaque ville** (groupby city), pour éviter les fuites entre villes.

| Label | Règle de construction | Taux de positifs |
|---|---|---|
| `rain_tomorrow` | `rainfall_J+1 > 1.0 mm` | **26.2 %** |
| `max_temp_tomorrow` | `max_temp_J+1` (valeur brute) | — (régression) |
| `weather_type_tomorrow` | Code WMO J+1 → `Sunny / Cloudy / Rainy / Stormy` | 3 classes |
| `heatwave_risk` | `max_temp_J+1 ≥ 35°C` **ET** `consec_hot_days ≥ 1` | **3.8 %** |
| `frost_risk` | `min_temp_J+1 ≤ 2°C` | **2.7 %** |
| `storm_probability` | `rainfall_J+1 > 10 mm` **ET** `wind_gust_J+1 > 50 km/h` | **2.2 %** |

La définition de `heatwave_risk` est plus stricte qu'un simple seuil de température : elle exige qu'il fasse déjà chaud *aujourd'hui* (au moins un jour chaud consécutif), ce qui réduit les faux positifs sur des pics isolés.

---

## 3. Features et feature engineering

Le pipeline utilise **46 features** (40 numériques + 6 catégorielles), calculées dans `pipeline/process_weather.py::add_features()`.

### Features météo brutes (mesures Open-Meteo)

| Feature | Description | Type |
|---|---|---|
| `min_temp` | Température minimale du jour (°C) | Numérique |
| `max_temp` | Température maximale du jour (°C) | Numérique |
| `temp_9am` / `temp_3pm` | Température à 9h et 15h locales (°C) | Numérique |
| `rainfall` | Précipitations du jour (mm) | Numérique |
| `evaporation` | Évapotranspiration (mm) | Numérique |
| `sunshine_hours` | Heures d'ensoleillement | Numérique |
| `wind_gust_speed` | Vitesse maximale des rafales (km/h) | Numérique |
| `wind_speed_9am` / `wind_speed_3pm` | Vitesse du vent à 9h et 15h (km/h) | Numérique |
| `humidity_9am` / `humidity_3pm` | Humidité relative à 9h et 15h (%) | Numérique |
| `pressure_9am` / `pressure_3pm` | Pression atmosphérique à 9h et 15h (hPa) | Numérique |
| `cloud_9am` / `cloud_3pm` | Couverture nuageuse à 9h et 15h (oktas 0–8) | Numérique |
| `rain_today` | Indicateur de pluie aujourd'hui (0/1) | Numérique |
| `precipitation_hours` | Durée de précipitation du jour (h) | Numérique |
| `shortwave_radiation_sum` | Rayonnement solaire total (Wh/m²) | Numérique |
| `wind_gust_dir` / `wind_dir_9am` / `wind_dir_3pm` | Direction du vent (16 points cardinaux) | Catégoriel |

### Features dérivées temporelles (feature engineering)

Ces features ne proviennent pas directement de l'API : elles sont calculées à partir de l'historique par ville.

| Feature | Calcul | Intérêt |
|---|---|---|
| `max_temp_lag1` / `lag2` | `max_temp` à J-1 et J-2 | Inertie thermique |
| `min_temp_lag1` | `min_temp` à J-1 | Signal gel persistant |
| `rainfall_lag1` / `lag2` | Précipitations à J-1 et J-2 | Saturation des sols |
| `pressure_3pm_lag1` | Pression à J-1 | Base du calcul de tendance |
| `max_temp_rolling7` | Moyenne mobile 7j de `max_temp` (shift 1) | Climatologie locale récente |
| `rainfall_rolling7` | Moyenne mobile 7j des précipitations | Régime pluvieux |
| `humidity_3pm_rolling7` | Moyenne mobile 7j de l'humidité 15h | Tendance d'humidité |
| `pressure_tendency` | `pressure_3pm - pressure_3pm_lag1` | Chute de pression → risque orage |
| `temp_anomaly` | `max_temp - max_temp_rolling7` | Écart à la "normale" locale |
| `consec_hot_days` | Somme de `hot_day + hot_day_lag1 + hot_day_lag2` | Compteur de jours chauds consécutifs |
| `hot_day_lag1` / `lag2` | Indicateur jour chaud (≥35°C) à J-1 et J-2 | Précurseurs canicule |
| `rainfall_intensity` | `rainfall / precipitation_hours` (mm/h) | Distingue l'averse courte de la bruine longue |

> La fenêtre glissante utilise `shift(1)` avant le `rolling(7)` pour éviter toute fuite de données (la moyenne des 7 jours précédents, pas incluant le jour J).

### Features de vapeur d'eau et vent haute altitude

| Feature | Description |
|---|---|
| `vpd_9am` / `vpd_3pm` | Déficit de pression de vapeur à 9h et 15h (hPa) — sécheresse de l'air |
| `wind_speed_100m_9am` / `wind_speed_100m_3pm` | Vitesse du vent à 100 m d'altitude (km/h) |

### Features géographiques et calendaires

| Feature | Description |
|---|---|
| `city` | Ville (26 modalités) | 
| `state` | État australien (7 modalités) |
| `month` | Mois (1–12) |
| `day_of_year` | Jour de l'année (1–366) |
| `season` | Saison australe : Summer / Autumn / Winter / Spring |

---

## 4. Importance des features (XGBoost)

Les importances ci-dessous sont les valeurs réelles extraites des modèles entraînés (`models/*_feature_importance.json`), calculées sur le **gain moyen** au sens XGBoost (réduction d'impureté pondérée par le nombre de splits).

### rain_tomorrow

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `rain_today` | 0.677 | La pluie est persistante ; si il pleut aujourd'hui, c'est le signal le plus fort |
| 2 | `rainfall` | 0.106 | Volume d'aujourd'hui corrèle avec demain |
| 3 | `pressure_tendency` | 0.019 | Chute de pression → front approchant |
| 4 | `wind_gust_dir` | 0.019 | Direction du vent porteur de pluie (ex : N-O en Australie) |
| 5 | `cloud_3pm` | 0.012 | Couverture nuageuse à 15h, proche du crépuscule |
| 6 | `humidity_3pm` | 0.011 | Humidité élevée en début de soirée |
| 7 | `wind_dir_3pm` | 0.009 | Direction du vent après-midi |
| 8 | `min_temp` | 0.009 | Nuits douces favorisent précipitations le lendemain |
| 9 | `pressure_3pm` | 0.008 | Valeur absolue de la pression |
| 10 | `rainfall_rolling7` | 0.008 | Régime pluvieux des 7 derniers jours |

### max_temp_tomorrow

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `max_temp` | 0.543 | Autocorrélation forte des températures |
| 2 | `max_temp_rolling7` | 0.174 | Tendance thermique sur 7 jours |
| 3 | `consec_hot_days` | 0.090 | Les vagues de chaleur s'auto-entretiennent |
| 4 | `max_temp_lag1` | 0.049 | Température hier |
| 5 | `temp_3pm` | 0.016 | Mesure à 15h, proche du maximum diurne |
| 6 | `rain_today` | 0.012 | La pluie refroidit le lendemain (albédo, évaporation) |
| 7 | `pressure_tendency` | 0.009 | Front froid → chute de température |
| 8 | `rainfall` | 0.009 | Volume de pluie corrèle avec refroidissement |
| 9 | `temp_anomaly` | 0.009 | Écart à la normale signal de persistance |
| 10 | `wind_gust_speed` | 0.008 | Vent fort → advection d'air froid |

### weather_type_tomorrow

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `rain_today` | 0.412 | Persistance du type météo |
| 2 | `wind_dir_9am` | 0.325 | Direction du flux dominant classe le type |
| 3 | `wind_dir_3pm` | 0.064 | Évolution de la direction en journée |
| 4 | `rainfall` | 0.064 | Volume → Rainy vs Stormy |
| 5 | `humidity_3pm` | 0.010 | Humidité après-midi |
| 6 | `cloud_3pm` | 0.010 | Nébulosité en fin de journée |
| 7 | `pressure_3pm_lag1` | 0.009 | Pression hier après-midi |
| 8 | `pressure_tendency` | 0.008 | Tendance barométrique |
| 9 | `pressure_3pm` | 0.006 | Valeur absolue de la pression |
| 10 | `wind_speed_3pm` | 0.006 | Intensité du vent à 15h |

### heatwave_risk

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `consec_hot_days` | 0.641 | Compteur de jours consécutifs chauds — feature dominante |
| 2 | `max_temp` | 0.093 | Température du jour |
| 3 | `max_temp_lag1` | 0.083 | Température hier |
| 4 | `hot_day_lag2` | 0.055 | Indicateur jour chaud il y a 2 jours |
| 5 | `max_temp_rolling7` | 0.014 | Tendance de fond |
| 6 | `max_temp_lag2` | 0.011 | Température il y a 2 jours |
| 7 | `hot_day_lag1` | 0.011 | Indicateur jour chaud hier |
| 8 | `temp_3pm` | 0.010 | Pic thermique de l'après-midi |
| 9 | `humidity_3pm` | 0.008 | Faible humidité = chaleur sèche persistante |
| 10 | `temp_anomaly` | 0.006 | Écart à la "normale" 7 jours |

### frost_risk

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `min_temp` | 0.472 | Température minimale du jour — signal direct du gel |
| 2 | `temp_9am` | 0.132 | Température de début de matinée |
| 3 | `max_temp` | 0.038 | Amplitude diurne |
| 4 | `temp_3pm` | 0.038 | Température de l'après-midi |
| 5 | `state` | 0.026 | Géographie : Victoria, Tasmanie plus exposées |
| 6 | `wind_dir_9am` | 0.022 | Flux froid du Sud |
| 7 | `min_temp_lag1` | 0.019 | Persistance du gel |
| 8 | `wind_dir_3pm` | 0.019 | Direction du vent en journée |
| 9 | `city` | 0.016 | Localisation : altitude, latitude |
| 10 | `season` | 0.013 | Hiver austral fortement corrélé |

### storm_probability

| Rang | Feature | Importance (gain) | Interprétation |
|---|---|---|---|
| 1 | `rain_today` | 0.134 | Système actif aujourd'hui → peut se prolonger |
| 2 | `rainfall` | 0.071 | Volume de précipitations |
| 3 | `hot_day_lag1` | 0.048 | Chaleur hier → instabilité convective |
| 4 | `wind_gust_speed` | 0.048 | Rafales actuelles = précurseur |
| 5 | `wind_dir_3pm` | 0.039 | Flux instable |
| 6 | `cloud_3pm` | 0.039 | Nébulosité élevée → convection |
| 7 | `pressure_tendency` | 0.038 | Chute rapide de pression = signal le plus classique |
| 8 | `wind_gust_dir` | 0.035 | Direction des rafales |
| 9 | `state` | 0.030 | Queensland et NSW : ceinture orageuse |
| 10 | `pressure_3pm` | 0.029 | Valeur absolue basse → dépression active |

---

## 5. Architecture des modèles

### Pourquoi XGBoost ?

XGBoost est bien adapté à ce problème pour plusieurs raisons : les features météorologiques présentent des interactions non-linéaires complexes (ex. : gel nécessite à la fois une température basse ET un ciel dégagé la nuit), les données contiennent des valeurs manquantes que XGBoost gère nativement, et les volumes (~26 villes × plusieurs années = quelques centaines de milliers de lignes) ne justifient pas un réseau de neurones. Des alternatives comme Random Forest ou LightGBM auraient pu fonctionner, mais XGBoost offre le meilleur équilibre performance / interprétabilité / rapidité d'entraînement sur ce type de données tabulaires.

### Hyperparamètres (depuis `config/modeling.yaml`)

Les paramètres de base sont partagés par tous les modèles, avec des surcharges par modèle :

| Paramètre | Valeur de base | Justification |
|---|---|---|
| `n_estimators` | 300 | Compromis performance/temps sur ce volume |
| `max_depth` | 6 | Limite le surapprentissage sur 46 features |
| `learning_rate` | 0.05 | Taux faible → convergence plus stable avec 300 arbres |
| `subsample` | 0.8 | 80 % des lignes par arbre → régularisation stochastique |
| `colsample_bytree` | 0.8 | 80 % des features par arbre → évite la domination d'une seule feature |

| Modèle | Surcharges | Raison |
|---|---|---|
| `weather_type_tomorrow` | `n_estimators: 400` | 3 classes, problème plus complexe |
| `heatwave_risk` | `max_depth: 4, n_estimators: 200` | Positifs rares (3.8 %), arbres moins profonds réduisent l'overfitting |
| `frost_risk` | `max_depth: 4, n_estimators: 200` | Même raison (2.7 % positifs) |

### Gestion du déséquilibre de classes

Les modèles binaires utilisent `scale_pos_weight = n_négatifs / n_positifs`, calculé dynamiquement à l'entraînement. Cela rééquilibre les gradients sans sur-échantillonner les données : pour `storm_probability` (2.2 % positifs), le poids est d'environ 44, ce qui force le modèle à mieux pénaliser les faux négatifs.

---

## 6. Pipeline d'entraînement

```
données brutes SQLite (weather_raw)
         │
         ▼
  add_features()              ← lags, rolling means, labels J+1
  encode_categoricals()       ← label-encoding des directions et villes
  get_feature_matrix()        ← 46 features, imputation par médiane
         │
         ▼
  train_test_split (80/20)    ← stratifié pour les binaires
         │
    ┌────┴────┐
   Train    Test
    │
    ▼
  XGBClassifier / XGBRegressor
  + scale_pos_weight (classes rares)
    │
    ▼
  Évaluation sur test set
  → metrics.json
    │
    ▼
  MLflow logging              ← params, métriques, feature importance, modèle
    │
    ▼
  compare_vs_baseline()       ← nouveau vs models/baseline/
    │
   ┌┴─────────────────┐
  ✓ Accepté          ✗ Rejeté (dégradation > seuil)
  → models/*.pkl       → rollback models/baseline/ + alerte Slack
```

### Split train/test

Le split est **aléatoire à 20 %** (seed=42), stratifié sur la variable cible pour les classifications. Il s'agit d'une **limite connue** : pour des séries temporelles météorologiques, un split temporel (entraîner sur J1–J3, tester sur J3–J4) serait plus rigoureux et éviterait toute fuite temporelle potentielle entre les lignes J-1 et J+1 d'une même vague de chaleur. En pratique, les lags et rolling means utilisent `shift(1)` avant le calcul, ce qui limite la fuite directe, mais le risque n'est pas nul.

### Validation contre la baseline

Après entraînement, le train DAG compare les métriques du nouveau modèle aux métriques de `models/baseline/` (snapshot du dernier modèle validé). Les seuils de tolérance sont versionnés dans `config/mlops.yaml` :

- `rain_accuracy` : régression acceptable ≤ 2 points de pourcentage
- `temp_MAE` : dégradation acceptable ≤ 0.2°C

Si le nouveau modèle est **plus mauvais** que ces tolérances, les fichiers `models/baseline/` sont restaurés dans `models/` et une alerte est émise. Cette gate évite qu'un retrain déclenché par drift ne dégrade les performances en production.

---

## 7. Métriques de performance

### Résultats actuels

| Modèle | Métrique principale | Valeur | Métrique secondaire | Valeur |
|---|---|---|---|---|
| `rain_tomorrow` | AUC-ROC | **0.855** | Accuracy | 0.770 |
| `max_temp_tomorrow` | MAE | **1.6°C** | R² | 0.907 |
| `weather_type_tomorrow` | F1-macro | **0.842** | Accuracy | 0.774 |
| `heatwave_risk` | AUC-ROC | **0.996** | F1 | 0.764 |
| `frost_risk` | AUC-ROC | **0.989** | F1 | 0.488 |
| `storm_probability` | AUC-ROC | **0.886** | F1 | 0.194 |

### Pourquoi ces métriques ?

**Pour `rain_tomorrow`**, la métrique principale est l'AUC-ROC, pas l'accuracy. Avec 26 % de positifs, un classifieur naïf "jamais de pluie" atteindrait 74 % d'accuracy. L'AUC mesure la capacité discriminante sur tous les seuils, ce qui est plus pertinent. Le F1 (0.64) traduit la difficulté à capturer les jours de pluie sans trop de faux positifs.

**Pour `max_temp_tomorrow`**, le MAE est préféré au RMSE car il est plus interprétable (« en moyenne, l'erreur est de 1.6°C ») et moins sensible aux outliers météorologiques extrêmes. Un R² de 0.907 sur 26 villes aux microclimats très différents est un résultat solide.

**Pour les événements rares** (`heatwave_risk`, `frost_risk`, `storm_probability`), l'AUC est la métrique principale car elle est indépendante du seuil de classification, et les classes sont très déséquilibrées (2–4 % de positifs). Le F1 faible de `storm_probability` (0.19) est attendu : avec 2.2 % de positifs, même une bonne discrimination AUC donne un F1 bas si le seuil n'est pas calibré.

---

## 8. Monitoring et retraining

### Architecture du monitoring quotidien

Un DAG Airflow s'exécute chaque jour et produit une décision en 4 états :

| État | Condition | Action |
|---|---|---|
| `trigger_retrain` | Accuracy < 75 % **ou** MAE > 3.0°C **ou** ≥ 4 features en drift | Lance le train DAG |
| `alert_only` | 3 features en drift (drift saisonnier probable) | Log + alerte Slack |
| `alert_insufficient_data` | < 30 observations sur la fenêtre | Log + alerte |
| `no_action` | Tous les seuils OK | Silencieux |

### Détection du drift — test de Kolmogorov-Smirnov

Le drift est détecté via un test KS (non-paramétrique) comparant la distribution des 30 derniers jours aux données d'entraînement, sur 6 features : `max_temp`, `min_temp`, `humidity_3pm`, `pressure_3pm`, `rainfall`, `wind_gust_speed`. Ces 6 variables sont toutes saisonnières par nature, d'où la distinction entre 3 features en drift (saisonnalité normale → `alert_only`) et 4+ features (drift structurel → retrain).

**Limite connue :** avec 6 tests indépendants au seuil α = 0.05, le taux de faux positifs global est ~26 % (1 − 0.95⁶). Un seuil de Bonferroni à 0.008 serait plus rigoureux en production.

### Fenêtre glissante et cooldown

- Fenêtre principale : **30 jours** pour les métriques et le test KS
- Alerte précoce : **7 jours** (seuils plus stricts : accuracy < 70 %, MAE > 4°C) — Slack uniquement, pas de retrain
- Cooldown entre retrains : **7 jours** minimum — évite les boucles de retraining

### Backtesting prévu vs réalisé

Le mart dbt `mart_forecast_vs_actual_timeline` calcule la performance passée en alignant `weather_predictions.max_temp_tomorrow` avec `weather_raw.max_temp` du lendemain. L'alignement est explicite (jointure sur `date + 1 = date_actuals`), ce qui évite l'erreur classique de comparer la prédiction de J avec la mesure de J au lieu de J+1. Des marts dédiés analysent les performances par saison (`mart_seasonal_performance`), par ville (`mart_model_performance_by_city`), et par type d'événement extrême (`mart_extreme_events_performance`).

---

## 9. Limites connues et améliorations futures

### Limites actuelles

**Split aléatoire pour des séries temporelles.** Le split 80/20 aléatoire expose à une fuite temporelle partielle : une vague de chaleur de 5 jours peut avoir ses jours J-1 en train et J+2 en test, faisant apparaître les performances plus bonnes qu'elles ne le seraient en production. Un split temporel strict (ex. : entraînement sur 2015–2023, test sur 2024) serait plus honnête.

**Horizon 24 heures uniquement.** Le pipeline ne prédit pas J+2 ou J+3. Pour des usages nécessitant une visibilité plus longue (agricole, événementiel), un modèle multi-horizon (ou une cascade de modèles) serait nécessaire.

**Absence d'intervalles de confiance.** Les prédictions sont des valeurs ponctuelles. Un recruteur ML expérimenté notera l'absence de quantification d'incertitude : un intervalle de prédiction pour `max_temp_tomorrow` (ex. : XGBoost + conformal prediction) ou une calibration de probabilité pour les binaires (`rain_tomorrow_proba`) renforcerait la valeur opérationnelle.

**Données Open-Meteo gratuites.** L'API free tier impose un délai de 10 s entre les appels par ville. Les données historiques sont des réanalyses (ERA5), pas des mesures directes de stations — elles peuvent diverger des relevés locaux sur les microclimats côtiers ou d'altitude.

**Multiple testing non corrigé.** Le test KS sur 6 features sans correction de Bonferroni génère un taux de faux positifs élevé (~26 %). Cela se manifeste par des `alert_only` fréquents en début et fin de saison, sans qu'un retrain ne soit nécessaire.

**F1 faible sur les événements rares.** Le F1 de `storm_probability` (0.19) et `frost_risk` (0.49) reflète la difficulté inhérente des classes ultra-minoritaires (2–3 %), même avec `scale_pos_weight`. Des techniques comme SMOTE, des ensembles ou des coûts de classification asymétriques pourraient améliorer la sensibilité sur ces cas critiques.

### Roadmap d'améliorations

| Priorité | Amélioration | Impact attendu |
|---|---|---|
| HAUTE | Split temporel train/test | Métriques plus réalistes |
| HAUTE | Correction Bonferroni sur les tests KS | Moins de faux positifs de drift |
| MOYENNE | Features nouvelles : `shortwave_radiation_sum`, VPD | +performance `heatwave_risk`, `max_temp` |
| MOYENNE | Calibration des probabilités (Platt scaling) | `rain_tomorrow_proba` plus fiable |
| MOYENNE | Intervalles de confiance (conformal prediction) | Usabilité en production |
| BASSE | Modèles par saison | Moins de drift structurel |
| BASSE | Prédiction multi-horizon (J+2, J+3) | Cas d'usage agricole/événementiel |
| BASSE | Migration BigQuery + dbt Cloud | Scalabilité à 100+ villes |
| BASSE | Températures à 850 hPa (endpoint pression Open-Meteo) | Précurseurs storm plus physiques |

---

*Document généré depuis les fichiers versionnés du projet (`config/modeling.yaml`, `models/metrics.json`, `models/*_feature_importance.json`) — les métriques et importances sont les vraies valeurs du modèle en production.*
