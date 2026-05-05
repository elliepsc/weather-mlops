-- Monthly performance broken down by city.
-- Surfaces per-city degradation that global averages mask.

WITH by_city AS (
    SELECT
        CAST({{ dbt.date_trunc('month', 'prediction_date') }} AS DATE) AS month,
        city,
        COUNT(*)                                            AS n_days,

        ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4)         AS rain_accuracy,
        ROUND(AVG(temp_abs_error), 3)                       AS temp_mae,
        ROUND(MAX(temp_abs_error), 3)                       AS temp_max_error,

        -- 30d rolling figures from last day of month (most recent snapshot)
        MAX(rain_accuracy_30d)                              AS rain_accuracy_30d_eom,
        MAX(temp_mae_30d)                                   AS temp_mae_30d_eom

    FROM {{ ref('int_model_performance_daily') }}
    GROUP BY CAST({{ dbt.date_trunc('month', 'prediction_date') }} AS DATE), city
)

SELECT
    b.*,
    dc.state,
    dc.latitude,
    dc.longitude
FROM by_city b
LEFT JOIN {{ ref('dim_cities') }} dc USING (city)
ORDER BY month DESC, city
