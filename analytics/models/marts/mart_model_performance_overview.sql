-- Monthly global performance across all cities.
-- Executive MLOps dashboard — one row per month.

WITH monthly AS (
    SELECT
        {{ dbt.date_trunc('month', 'prediction_date') }}    AS month,
        COUNT(*)                                            AS n_city_days,
        COUNT(DISTINCT prediction_date)                     AS n_days,
        COUNT(DISTINCT city)                                AS n_cities,

        ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4)         AS rain_accuracy,
        ROUND(AVG(temp_abs_error), 3)                       AS temp_mae,
        ROUND(MAX(temp_abs_error), 3)                       AS temp_max_error,
        ROUND(MIN(temp_abs_error), 3)                       AS temp_min_error,

        -- Coverage: share of city-days where actuals were available
        ROUND(
            100.0 * SUM(CASE WHEN has_actuals THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            1
        )                                                   AS pct_with_actuals

    FROM {{ ref('int_actuals_vs_predictions') }}
    GROUP BY {{ dbt.date_trunc('month', 'prediction_date') }}
)

SELECT *
FROM monthly
ORDER BY month DESC
