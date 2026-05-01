// ============================================================
// weather-mlops — Power Query M Template
//
// PARAMÈTRE : DataMode
//   "github"    → lecture depuis GitHub raw URL (mode portfolio / cloud)
//   "local"     → lecture depuis chemin local    (mode développement)
//   "bigquery"  → lecture depuis BigQuery         (Phase 2 GCP)
//
// Pour changer de mode dans Power BI Desktop :
//   1. Accueil → Transformer les données
//   2. Dans le volet gauche, sélectionner "DataMode"
//   3. Modifier la valeur dans la barre de formule
//   4. Fermer & Appliquer
//   5. Actualiser les données
//
// Pour créer les paramètres dans Power BI Desktop (première fois) :
//   Accueil → Gérer les paramètres → Nouveau paramètre
//   Répéter pour DataMode, LocalBasePath, BQProject, BQDataset
// ============================================================


// ============================================================
// PARAMÈTRE : DataMode
// Type: Texte | Valeur par défaut: "github"
// Valeurs suggérées: github | local | bigquery
// ============================================================
// Coller dans une requête nommée "DataMode" → marquer comme Paramètre
"github"


// ============================================================
// PARAMÈTRE : LocalBasePath
// Type: Texte | Valeur par défaut: "C:/Users/xxx/weather-mlops/data/analytics/"
// Utilisé uniquement si DataMode = "local"
// ============================================================
// Coller dans une requête nommée "LocalBasePath" → marquer comme Paramètre
"C:/Users/xxx/weather-mlops/data/analytics/"


// ============================================================
// PARAMÈTRE : BQProject
// Type: Texte | Valeur par défaut: "your-gcp-project"
// Utilisé uniquement si DataMode = "bigquery"
// ============================================================
// Coller dans une requête nommée "BQProject" → marquer comme Paramètre
"your-gcp-project"


// ============================================================
// PARAMÈTRE : BQDataset
// Type: Texte | Valeur par défaut: "weather_mlops"
// Utilisé uniquement si DataMode = "bigquery"
// ============================================================
// Coller dans une requête nommée "BQDataset" → marquer comme Paramètre
"weather_mlops"


// ============================================================
// FONCTION HELPER : GetSource(martName)
// Retourne une table non typée (CSV) ou typée (BigQuery) selon DataMode.
// Coller dans une requête nommée "GetSource" → désactiver "Activer le chargement"
// ============================================================
(martName as text) as table =>
let
    GHBase = "https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/",

    Result =
        if DataMode = "local" then
            let
                Raw    = File.Contents(LocalBasePath & martName & ".csv"),
                Doc    = Csv.Document(Raw, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),
                Headed = Table.PromoteHeaders(Doc, [PromoteAllScalars = true])
            in Headed
        else if DataMode = "bigquery" then
            let
                DB  = GoogleBigQuery.Database(BQProject, BQDataset),
                Tbl = DB{[Schema = BQDataset, Item = martName]}[Data]
            in Tbl
        else
            // Mode "github" par défaut
            let
                Raw    = Web.Contents(GHBase & martName & ".csv"),
                Doc    = Csv.Document(Raw, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),
                Headed = Table.PromoteHeaders(Doc, [PromoteAllScalars = true])
            in Headed
in
    Result


// ============================================================
// REQUÊTE : mart_forecast_vs_actual_timeline
// Grain : ville × jour (90j) — Prédictions vs actuals alignés J+1
// Coller dans une requête nommée "mart_forecast_vs_actual_timeline"
// ============================================================
let
    Source = GetSource("mart_forecast_vs_actual_timeline"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"prediction_date",           type date},
        {"city",                      type text},
        {"state",                     type text},
        {"pred_rain_tomorrow",        type logical},
        {"pred_rain_proba",           type number},
        {"pred_max_temp_tomorrow",    type number},
        {"pred_weather_type_tomorrow",type text},
        {"pred_heatwave_risk",        type logical},
        {"pred_frost_risk",           type logical},
        {"pred_storm_probability",    type number},
        {"comfort_score",             type number},
        {"actual_date",               type date},
        {"actual_rain",               type logical},
        {"actual_max_temp",           type number},
        {"actual_min_temp",           type number},
        {"actual_rainfall",           type number},
        {"rain_correct",              type logical},
        {"temp_abs_error",            type number},
        {"has_actuals",               type logical},
        {"rain_accuracy_30d",         type number},
        {"temp_mae_30d",              type number},
        {"rain_accuracy_7d",          type number},
        {"temp_mae_7d",               type number}
    })
in
    Typed


// ============================================================
// REQUÊTE : mart_model_performance_overview
// Grain : mois — Métriques globales tous modèles
// Coller dans une requête nommée "mart_model_performance_overview"
// ============================================================
let
    Source = GetSource("mart_model_performance_overview"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"month",           type date},
        {"n_city_days",     Int64.Type},
        {"n_days",          Int64.Type},
        {"n_cities",        Int64.Type},
        {"rain_accuracy",   type number},
        {"temp_mae",        type number},
        {"temp_max_error",  type number},
        {"temp_min_error",  type number},
        {"pct_with_actuals",type number}
    })
in
    Typed


// ============================================================
// REQUÊTE : mart_model_performance_by_city
// Grain : ville × mois — Dégradation par ville
// Coller dans une requête nommée "mart_model_performance_by_city"
// ============================================================
let
    Source = GetSource("mart_model_performance_by_city"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"month",               type date},
        {"city",                type text},
        {"n_days",              Int64.Type},
        {"rain_accuracy",       type number},
        {"temp_mae",            type number},
        {"temp_max_error",      type number},
        {"rain_accuracy_30d_eom",type number},
        {"temp_mae_30d_eom",    type number},
        {"state",               type text},
        {"latitude",            type number},
        {"longitude",           type number}
    })
in
    Typed


// ============================================================
// REQUÊTE : mart_mlops_health
// Grain : jour (30j) — Complétude ingestion + décisions monitoring
// Coller dans une requête nommée "mart_mlops_health"
// ============================================================
let
    Source = GetSource("mart_mlops_health"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"date",               type date},
        {"completeness_pct",   type number},
        {"missing_city_count", Int64.Type},
        {"present_cities",     Int64.Type},
        {"expected_cities",    Int64.Type},
        {"has_gap",            type logical},
        {"monitoring_action",  type text},
        {"monitoring_reason",  type text},
        {"n_drifted_features", Int64.Type},
        {"heavy_drift",        type logical},
        {"mild_drift",         type logical},
        {"low_accuracy",       type logical},
        {"high_mae",           type logical},
        {"rain_accuracy_30d",  type number},
        {"temp_mae_30d",       type number},
        {"last_retrain_date",  type date},
        {"days_since_retrain", Int64.Type}
    })
in
    Typed


// ============================================================
// REQUÊTE : mart_retraining_history
// Grain : événement retrain — Audit avant/après
// Coller dans une requête nommée "mart_retraining_history"
// ============================================================
let
    Source = GetSource("mart_retraining_history"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"retrain_date",                    type date},
        {"source",                          type text},
        {"trigger_action",                  type text},
        {"trigger_reason",                  type text},
        {"n_drifted_features",              Int64.Type},
        {"heavy_drift",                     type logical},
        {"low_accuracy",                    type logical},
        {"preceded_by_gap",                 type logical},
        {"gap_days_in_7d_window",           Int64.Type},
        {"max_missing_cities_7d",           Int64.Type},
        {"avg_completeness_7d_pct",         type number},
        {"rain_accuracy_at_retrain",        type number},
        {"temp_mae_at_retrain",             type number},
        {"next_retrain_date",               type date},
        {"next_rain_accuracy",              type number},
        {"next_temp_mae",                   type number},
        {"accuracy_delta",                  type number},
        {"mae_delta",                       type number},
        {"accuracy_degraded_after_retrain", type logical}
    })
in
    Typed
