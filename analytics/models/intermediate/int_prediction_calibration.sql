-- Calibration analysis for pred_rain_proba.
-- Answers: "Does a predicted probability of 70% correspond to 70% actual rain?"
-- Methodology: isotonic-style bucket analysis (10 buckets of 10pp each).
-- This is the foundation for the reliability diagram in mart_model_calibration.
--
-- IMPORTANT: pred_rain_proba is a real-valued score [0, 1].
-- The existing models only check binary rain_correct; they ignore calibration.
-- A model with 80% accuracy but poor calibration is dangerous for downstream
-- risk scoring (heatwave_risk, storm_probability are also produced by the same
-- model family and likely share the same calibration defect).

WITH base AS (
    SELECT
        p.prediction_date,
        p.city,
        p.pred_rain_proba,
        p.actual_rain,
        d.season_southern   AS season,
        d.year,
        d.month

    FROM {{ ref('int_actuals_vs_predictions') }} p
    LEFT JOIN {{ ref('dim_date') }} d
        ON d.date = p.prediction_date
    WHERE p.has_actuals
      AND p.pred_rain_proba IS NOT NULL
),

bucketed AS (
    SELECT
        *,
        -- 10 equal-width buckets: [0,0.1), [0.1,0.2) … [0.9,1.0]
        FLOOR(pred_rain_proba * 10) / 10.0          AS prob_bucket_low,
        LEAST(FLOOR(pred_rain_proba * 10) / 10.0 + 0.1, 1.0) AS prob_bucket_high,
        -- Midpoint of bucket for plotting
        FLOOR(pred_rain_proba * 10) / 10.0 + 0.05  AS prob_bucket_mid
    FROM base
)

SELECT
    prob_bucket_low,
    prob_bucket_high,
    prob_bucket_mid,
    season,

    COUNT(*)                                AS n_predictions,
    AVG(pred_rain_proba)                    AS mean_predicted_proba,
    -- Observed fraction = actual calibration target
    AVG(CAST(actual_rain AS DOUBLE))        AS observed_rain_rate,
    -- Calibration error per bucket
    AVG(pred_rain_proba)
        - AVG(CAST(actual_rain AS DOUBLE))  AS calibration_error,
    ABS(
        AVG(pred_rain_proba)
        - AVG(CAST(actual_rain AS DOUBLE))
    )                                       AS abs_calibration_error,

    -- Brier score contribution (MSE of probability vs binary outcome)
    AVG(
        POWER(pred_rain_proba - CAST(actual_rain AS DOUBLE), 2)
    )                                       AS brier_score_bucket,

    -- Over-confident flag: model says high probability but low actual rain
    CASE
        WHEN AVG(pred_rain_proba) - AVG(CAST(actual_rain AS DOUBLE)) > 0.1
        THEN TRUE ELSE FALSE
    END                                     AS overconfident,

    -- Under-confident flag
    CASE
        WHEN AVG(CAST(actual_rain AS DOUBLE)) - AVG(pred_rain_proba) > 0.1
        THEN TRUE ELSE FALSE
    END                                     AS underconfident

FROM bucketed
GROUP BY prob_bucket_low, prob_bucket_high, prob_bucket_mid, season
ORDER BY prob_bucket_low, season
