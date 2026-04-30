-- Signed prediction errors per city × season.
-- The existing models only compute MAE (unsigned) for temperature.
-- Signed error reveals systematic bias: a model with MAE=2°C that is
-- consistently +2°C hot is fundamentally different from one that is ±2°C random.
-- Both bias directions matter for operational decisions (e.g. heatwave alerts).

WITH base AS (
    SELECT
        p.prediction_date,
        p.city,
        p.pred_max_temp_tomorrow,
        p.actual_max_temp,
        p.pred_rain_proba,
        p.pred_rain_tomorrow,
        p.actual_rain,
        p.rain_correct,
        p.temp_abs_error,
        d.au_season,
        d.year,
        d.month

    FROM {{ ref('int_actuals_vs_predictions') }} p
    LEFT JOIN {{ ref('dim_date') }} d
        ON d.date = p.prediction_date
    WHERE p.has_actuals
)

SELECT
    city,
    au_season,
    year,

    COUNT(*)                                            AS n_days,

    -- Temperature bias (positive = model overestimates)
    ROUND(AVG(pred_max_temp_tomorrow - actual_max_temp), 3)     AS temp_mean_bias,
    ROUND(AVG(temp_abs_error), 3)                               AS temp_mae,
    ROUND(STDDEV(pred_max_temp_tomorrow - actual_max_temp), 3)  AS temp_bias_stddev,
    -- RMSE
    ROUND(
        SQRT(AVG(POWER(pred_max_temp_tomorrow - actual_max_temp, 2))),
        3
    )                                                           AS temp_rmse,
    -- P90 of absolute error (tail risk)
    PERCENTILE_CONT(0.90) WITHIN GROUP (
        ORDER BY temp_abs_error
    )                                                           AS temp_mae_p90,

    -- Rain bias: % of days with false positive (predicted rain, no actual rain)
    ROUND(
        100.0 * SUM(
            CASE WHEN pred_rain_tomorrow AND NOT actual_rain THEN 1 ELSE 0 END
        ) / NULLIF(COUNT(*), 0),
        2
    )                                                           AS rain_false_positive_pct,
    -- False negatives (missed rain)
    ROUND(
        100.0 * SUM(
            CASE WHEN NOT pred_rain_tomorrow AND actual_rain THEN 1 ELSE 0 END
        ) / NULLIF(COUNT(*), 0),
        2
    )                                                           AS rain_false_negative_pct,

    -- Summary bias direction for quick filtering
    CASE
        WHEN ABS(AVG(pred_max_temp_tomorrow - actual_max_temp)) < 0.5
        THEN 'Unbiased'
        WHEN AVG(pred_max_temp_tomorrow - actual_max_temp) > 0
        THEN 'Overestimates'
        ELSE 'Underestimates'
    END                                                         AS temp_bias_direction

FROM base
GROUP BY city, au_season, year
ORDER BY city, year, au_season
