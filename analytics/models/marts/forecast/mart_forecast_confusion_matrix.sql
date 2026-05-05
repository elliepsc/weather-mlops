-- Monthly confusion matrix per city: TP, TN, FP, FN + derived metrics.

WITH agg AS (
    SELECT
        date_trunc('month', prediction_date)    AS year_month,
        EXTRACT(YEAR  FROM prediction_date)     AS year,
        EXTRACT(MONTH FROM prediction_date)     AS month,
        city,
        state,

        SUM(CASE WHEN forecast_result = 'True positive'  THEN 1 ELSE 0 END) AS true_positive,
        SUM(CASE WHEN forecast_result = 'True negative'  THEN 1 ELSE 0 END) AS true_negative,
        SUM(CASE WHEN forecast_result = 'False positive' THEN 1 ELSE 0 END) AS false_positive,
        SUM(CASE WHEN forecast_result = 'False negative' THEN 1 ELSE 0 END) AS false_negative,
        COUNT(*)                                                              AS n_predictions
    FROM {{ ref('mart_forecast_vs_actual_daily') }}
    WHERE forecast_result <> 'Unknown'
    GROUP BY
        date_trunc('month', prediction_date),
        EXTRACT(YEAR FROM prediction_date),
        EXTRACT(MONTH FROM prediction_date),
        city, state
)

SELECT
    *,
    (true_positive + true_negative) * 1.0 / NULLIF(n_predictions, 0)       AS accuracy,
    true_positive * 1.0 / NULLIF(true_positive + false_positive, 0)         AS precision,
    true_positive * 1.0 / NULLIF(true_positive + false_negative, 0)         AS recall,
    2.0 * true_positive
        / NULLIF(2 * true_positive + false_positive + false_negative, 0)    AS f1_score,
    false_positive * 1.0 / NULLIF(false_positive + true_negative, 0)        AS false_positive_rate,
    false_negative * 1.0 / NULLIF(false_negative + true_positive, 0)        AS false_negative_rate
FROM agg
