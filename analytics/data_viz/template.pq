// Power Query M — Requêtes prêtes à coller dans Power BI Desktop
//
// Phase 1 : GitHub raw URL (source par défaut)
// Phase 2 migration BQ : remplacer Web.Contents
//   par GoogleBigQuery.Database("project-id", "dataset_id")
//   et sélectionner la table correspondante.
//
// Comment coller :
//   Power BI Desktop → Transformer les données → Éditeur Power Query
//   → Nouvelle source → Requête vide → Éditeur avancé → coller la requête

// ─────────────────────────────────────────────────────────────────────────────
// mart_model_performance_overview
// Grain : mois — Métriques globales de tous les modèles
// ─────────────────────────────────────────────────────────────────────────────
let
    Source = Web.Contents("https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_overview.csv"),
    Parsed = Csv.Document(Source, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Header = Table.PromoteHeaders(Parsed, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Header, {
        {"month",                  type date},
        {"rain_accuracy",          type number},
        {"rain_auc",               type number},
        {"max_temp_mae",           type number},
        {"max_temp_r2",            type number},
        {"weather_type_accuracy",  type number},
        {"heatwave_auc",           type number},
        {"frost_auc",              type number},
        {"storm_auc",              type number},
        {"row_count",              Int64.Type}
    })
in
    Typed

// ─────────────────────────────────────────────────────────────────────────────
// mart_model_performance_by_city
// Grain : ville × mois — Dégradation des métriques par ville
// ─────────────────────────────────────────────────────────────────────────────
let
    Source = Web.Contents("https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_model_performance_by_city.csv"),
    Parsed = Csv.Document(Source, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Header = Table.PromoteHeaders(Parsed, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Header, {
        {"month",                  type date},
        {"city",                   type text},
        {"state",                  type text},
        {"rain_accuracy",          type number},
        {"rain_auc",               type number},
        {"max_temp_mae",           type number},
        {"weather_type_accuracy",  type number},
        {"row_count",              Int64.Type}
    })
in
    Typed

// ─────────────────────────────────────────────────────────────────────────────
// mart_forecast_vs_actual_timeline
// Grain : ville × jour (90j) — Prédictions vs réel alignés J+1
// ─────────────────────────────────────────────────────────────────────────────
let
    Source = Web.Contents("https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_forecast_vs_actual_timeline.csv"),
    Parsed = Csv.Document(Source, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Header = Table.PromoteHeaders(Parsed, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Header, {
        {"date",                    type date},
        {"city",                    type text},
        {"predicted_rain_tomorrow", type logical},
        {"actual_rain_tomorrow",    type logical},
        {"rain_correct",            type logical},
        {"predicted_max_temp",      type number},
        {"actual_max_temp",         type number},
        {"temp_error",              type number},
        {"predicted_weather_type",  type text},
        {"actual_weather_type",     type text},
        {"comfort_score",           type number},
        {"heatwave_risk",           type logical},
        {"frost_risk",              type logical}
    })
in
    Typed

// ─────────────────────────────────────────────────────────────────────────────
// mart_mlops_health
// Grain : jour (30j) — Complétude ingestion et décisions MLOps
// ─────────────────────────────────────────────────────────────────────────────
let
    Source = Web.Contents("https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_mlops_health.csv"),
    Parsed = Csv.Document(Source, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Header = Table.PromoteHeaders(Parsed, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Header, {
        {"date",                   type date},
        {"ingestion_completeness", type number},
        {"cities_ok",              Int64.Type},
        {"cities_total",           Int64.Type},
        {"monitoring_decision",    type text},
        {"drift_detected",         type logical},
        {"drift_score",            type number},
        {"data_sufficient",        type logical},
        {"cooldown_active",        type logical}
    })
in
    Typed

// ─────────────────────────────────────────────────────────────────────────────
// mart_retraining_history
// Grain : événement de retrain — Audit avant/après
// ─────────────────────────────────────────────────────────────────────────────
let
    Source = Web.Contents("https://raw.githubusercontent.com/elliepsc/weather-mlops/main/data/analytics/mart_retraining_history.csv"),
    Parsed = Csv.Document(Source, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    Header = Table.PromoteHeaders(Parsed, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Header, {
        {"retrain_date",       type date},
        {"trigger_reason",     type text},
        {"rain_auc_before",    type number},
        {"rain_auc_after",     type number},
        {"max_temp_mae_before", type number},
        {"max_temp_mae_after", type number},
        {"model_promoted",     type logical},
        {"rollback",           type logical},
        {"training_rows",      Int64.Type}
    })
in
    Typed
