-- Drift explainability: correlates detected feature drift with weather anomalies.
--
-- THE KEY QUESTION this mart answers:
--   "When the monitoring pipeline flags drift, is it because:
--     (A) the model degraded on normal data → action: retrain
--     (B) the weather was statistically unusual → action: monitor, do NOT retrain
--     (C) both → action: retrain AND investigate feature engineering"
--
-- Without this model, every drift event triggers the same response regardless
-- of whether Adelaide just had a record heatwave. This is operationally expensive
-- and statistically unsound (retraining on an outlier episode can degrade the
-- model on normal weather).
--
-- Grain: monitoring_date (= date of the drift report).
-- One row per date. Aggregates across all 26 cities to produce a daily
-- weather-anomaly context that can be read alongside stg_drift_reports.
--
-- HOW TO USE IN PRACTICE:
--   drift_days AND high_weather_anomaly  → likely (B) or (C): investigate first
--   drift_days AND NOT high_weather_anomaly → likely (A): retrain
--   no drift AND high_weather_anomaly    → model was robust to unusual weather

WITH drift AS (
    SELECT
        date,
        feature_name,
        ks_stat,
        drifted
    FROM {{ ref('stg_drift_reports') }}
),

-- Aggregate drift signals per monitoring date
drift_daily AS (
    SELECT
        date,
        COUNT(DISTINCT feature_name)            AS n_features_monitored,
        SUM(CASE WHEN drifted THEN 1 ELSE 0 END) AS n_drifted_features,
        ROUND(AVG(ks_stat), 4)                  AS mean_ks_stat,
        ROUND(MAX(ks_stat), 4)                  AS max_ks_stat
    FROM drift
    GROUP BY date
),

-- Aggregate weather anomaly scores across all cities for the same date
-- (drift reports cover all cities → compare to all-city anomaly composite)
anomaly_daily AS (
    SELECT
        date,
        COUNT(*)                                AS n_cities_observed,
        SUM(CASE WHEN is_anomalous_day THEN 1 ELSE 0 END)
                                                AS n_anomalous_cities,
        ROUND(AVG(composite_anomaly_score), 3)  AS avg_composite_anomaly,
        ROUND(MAX(composite_anomaly_score), 3)  AS max_composite_anomaly,
        ROUND(AVG(n_anomalous_features), 2)     AS avg_anomalous_features_per_city,

        -- Which features were most anomalous across cities?
        ROUND(AVG(ABS(z_max_temp)),    2)       AS avg_abs_z_max_temp,
        ROUND(AVG(ABS(z_rainfall)),    2)       AS avg_abs_z_rainfall,
        ROUND(AVG(ABS(z_wind_gust)),   2)       AS avg_abs_z_wind_gust,
        ROUND(AVG(ABS(z_humidity_3pm)),2)       AS avg_abs_z_humidity_3pm,
        ROUND(AVG(ABS(z_pressure_3pm)),2)       AS avg_abs_z_pressure_3pm
    FROM {{ ref('int_weather_anomaly_score') }}
    GROUP BY date
),

-- Monitoring decisions for context
decisions AS (
    SELECT
        date,
        action,
        reason,
        heavy_drift,
        low_accuracy
    FROM {{ ref('stg_monitoring_decisions') }}
),

combined AS (
    SELECT
        d.date,

        -- Drift context
        d.n_features_monitored,
        d.n_drifted_features,
        d.mean_ks_stat,
        d.max_ks_stat,
        d.n_drifted_features > 0                        AS any_drift,

        -- Weather anomaly context on the same date
        a.n_cities_observed,
        a.n_anomalous_cities,
        a.avg_composite_anomaly,
        a.max_composite_anomaly,
        a.avg_anomalous_features_per_city,
        a.avg_abs_z_max_temp,
        a.avg_abs_z_rainfall,
        a.avg_abs_z_wind_gust,

        -- Flag: was weather broadly anomalous that day?
        -- Threshold: >30% of cities anomalous OR avg composite > 2.0
        (
            a.n_anomalous_cities > 0.3 * a.n_cities_observed
            OR a.avg_composite_anomaly > 2.0
        )                                               AS high_weather_anomaly,

        -- Monitoring decision
        dec.action                                      AS monitoring_action,
        dec.heavy_drift,
        dec.low_accuracy,

        -- DIAGNOSTIC CLASSIFICATION (the core value of this mart)
        CASE
            WHEN d.n_drifted_features > 0
             AND (a.n_anomalous_cities > 0.3 * a.n_cities_observed
                  OR a.avg_composite_anomaly > 2.0)
            THEN 'Drift during anomalous weather — investigate before retraining'
            WHEN d.n_drifted_features > 0
             AND NOT (a.n_anomalous_cities > 0.3 * a.n_cities_observed
                  OR a.avg_composite_anomaly > 2.0)
            THEN 'Drift on normal weather — model degradation likely'
            WHEN d.n_drifted_features = 0
             AND (a.n_anomalous_cities > 0.3 * a.n_cities_observed
                  OR a.avg_composite_anomaly > 2.0)
            THEN 'No drift despite anomalous weather — model robust'
            ELSE 'No drift, normal weather — baseline'
        END                                             AS drift_explainability_label

    FROM drift_daily d
    LEFT JOIN anomaly_daily a ON a.date = d.date
    LEFT JOIN decisions dec   ON dec.date = d.date
)

SELECT *
FROM combined
ORDER BY date DESC
