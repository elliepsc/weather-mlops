-- Rain probability calibration by 10 % buckets per (year_month, city).
-- calibration_gap > 0 means model over-predicts rain; < 0 means under-predicts.

WITH bucketed AS (
    SELECT
        date_trunc('month', prediction_date)    AS year_month,
        EXTRACT(YEAR  FROM prediction_date)     AS year,
        EXTRACT(MONTH FROM prediction_date)     AS month,
        city,
        state,
        pred_rain_proba,
        actual_rain_tomorrow,
        CASE
            WHEN pred_rain_proba < 0.10 THEN '00-10%'
            WHEN pred_rain_proba < 0.20 THEN '10-20%'
            WHEN pred_rain_proba < 0.30 THEN '20-30%'
            WHEN pred_rain_proba < 0.40 THEN '30-40%'
            WHEN pred_rain_proba < 0.50 THEN '40-50%'
            WHEN pred_rain_proba < 0.60 THEN '50-60%'
            WHEN pred_rain_proba < 0.70 THEN '60-70%'
            WHEN pred_rain_proba < 0.80 THEN '70-80%'
            WHEN pred_rain_proba < 0.90 THEN '80-90%'
            ELSE '90-100%'
        END                                     AS proba_bucket
    FROM {{ ref('mart_forecast_vs_actual_daily') }}
    WHERE pred_rain_proba IS NOT NULL
      AND actual_rain_tomorrow IS NOT NULL
)

SELECT
    year_month,
    year,
    month,
    city,
    state,
    proba_bucket,
    COUNT(*)                                    AS n_predictions,
    AVG(pred_rain_proba)                        AS avg_predicted_probability,
    AVG(actual_rain_tomorrow)                   AS actual_rain_rate,
    AVG(pred_rain_proba) - AVG(actual_rain_tomorrow)                    AS calibration_gap,
    AVG(POW(pred_rain_proba - actual_rain_tomorrow, 2))                 AS brier_score
FROM bucketed
GROUP BY year_month, year, month, city, state, proba_bucket
