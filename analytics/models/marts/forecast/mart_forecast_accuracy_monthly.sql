-- Monthly forecast accuracy per city: rain accuracy, temperature MAE, bias.

SELECT
    date_trunc('month', prediction_date)    AS year_month,
    EXTRACT(YEAR  FROM prediction_date)     AS year,
    EXTRACT(MONTH FROM prediction_date)     AS month,
    city,
    state,

    COUNT(*)                                AS n_predictions,
    SUM(CASE WHEN actual_rain_tomorrow IS NOT NULL THEN 1 ELSE 0 END) AS n_rain_actuals,
    AVG(rain_prediction_correct)            AS rain_accuracy,
    AVG(temp_abs_error)                     AS temp_mae,
    MAX(temp_abs_error)                     AS temp_max_error,
    AVG(temp_error_signed)                  AS temp_bias,
    AVG(pred_rain_proba)                    AS avg_pred_rain_proba,
    AVG(actual_rain_tomorrow)               AS actual_rain_rate
FROM {{ ref('mart_forecast_vs_actual_daily') }}
GROUP BY
    date_trunc('month', prediction_date),
    EXTRACT(YEAR FROM prediction_date),
    EXTRACT(MONTH FROM prediction_date),
    city,
    state
