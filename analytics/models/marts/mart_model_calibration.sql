-- Reliability diagram data for rain probability predictions.
-- Exposes calibration quality across all seasons, globally and per season.
-- Key MLOps question: "Is a 70% rain probability actually meaningful?"
-- If calibration_error is consistently positive, the model is overconfident
-- → downstream risk scores (heatwave_risk, storm_probability) are also suspect.
--
-- Power BI usage: scatter plot of prob_bucket_mid (x) vs observed_rain_rate (y),
-- with the diagonal line (perfect calibration) as reference.

WITH global_calibration AS (
    SELECT
        prob_bucket_low,
        prob_bucket_high,
        prob_bucket_mid,
        'All seasons'                   AS season,
        SUM(n_predictions)              AS n_predictions,
        -- Weighted averages across seasons
        ROUND(
            SUM(mean_predicted_proba * n_predictions) / NULLIF(SUM(n_predictions), 0),
            4
        )                               AS mean_predicted_proba,
        ROUND(
            SUM(observed_rain_rate * n_predictions) / NULLIF(SUM(n_predictions), 0),
            4
        )                               AS observed_rain_rate,
        ROUND(
            SUM(brier_score_bucket * n_predictions) / NULLIF(SUM(n_predictions), 0),
            4
        )                               AS brier_score_bucket

    FROM {{ ref('int_prediction_calibration') }}
    GROUP BY prob_bucket_low, prob_bucket_high, prob_bucket_mid
),

seasonal_calibration AS (
    SELECT
        prob_bucket_low,
        prob_bucket_high,
        prob_bucket_mid,
        season,
        n_predictions,
        ROUND(mean_predicted_proba, 4)  AS mean_predicted_proba,
        ROUND(observed_rain_rate, 4)    AS observed_rain_rate,
        ROUND(brier_score_bucket, 4)    AS brier_score_bucket
    FROM {{ ref('int_prediction_calibration') }}
),

combined AS (
    SELECT * FROM global_calibration
    UNION ALL
    SELECT * FROM seasonal_calibration
)

SELECT
    *,
    ROUND(mean_predicted_proba - observed_rain_rate, 4)     AS calibration_error,
    ABS(mean_predicted_proba - observed_rain_rate) > 0.1    AS is_miscalibrated,
    CASE
        WHEN mean_predicted_proba - observed_rain_rate >  0.1 THEN 'Overconfident'
        WHEN observed_rain_rate - mean_predicted_proba >  0.1 THEN 'Underconfident'
        ELSE 'Calibrated'
    END                                                     AS calibration_verdict
FROM combined
ORDER BY season, prob_bucket_low
