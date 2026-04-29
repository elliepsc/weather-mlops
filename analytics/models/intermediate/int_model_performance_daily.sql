-- City × date grain with 30-day rolling performance metrics.
-- Provides finer view than the global 30d averages in monitoring_decision.json,
-- which masks per-city degradation.

WITH base AS (
    SELECT * FROM {{ ref('int_actuals_vs_predictions') }}
    WHERE has_actuals
)

SELECT
    prediction_date,
    city,
    actual_date,
    rain_correct,
    temp_abs_error,

    -- 30-day rolling accuracy per city
    AVG(CAST(rain_correct AS DOUBLE)) OVER (
        PARTITION BY city
        ORDER BY prediction_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                       AS rain_accuracy_30d,

    -- 30-day rolling MAE per city
    AVG(temp_abs_error) OVER (
        PARTITION BY city
        ORDER BY prediction_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                       AS temp_mae_30d,

    -- 7-day rolling for detecting rapid degradation
    AVG(CAST(rain_correct AS DOUBLE)) OVER (
        PARTITION BY city
        ORDER BY prediction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                       AS rain_accuracy_7d,

    AVG(temp_abs_error) OVER (
        PARTITION BY city
        ORDER BY prediction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                       AS temp_mae_7d

FROM base
