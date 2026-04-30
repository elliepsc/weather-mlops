-- Temperature prediction bias by city × season.
-- Designed for stakeholder presentation: surfaces which cities the model
-- systematically over- or underestimates, and whether bias is seasonal.
-- Complements mart_model_performance_by_city (which shows MAE only).
--
-- Decision use-case: persistent underestimation of max_temp in Summer
-- for coastal cities → heatwave alert thresholds should be adjusted.

WITH bias AS (
    SELECT * FROM {{ ref('int_prediction_bias_by_city') }}
),

with_geo AS (
    SELECT
        b.*,
        dc.state,
        dc.latitude,
        dc.longitude
    FROM bias b
    LEFT JOIN {{ ref('dim_cities') }} dc USING (city)
),

-- Aggregate to city × season (across all years) for a stable view
city_season AS (
    SELECT
        city,
        state,
        latitude,
        longitude,
        au_season,

        SUM(n_days)                                 AS n_days_total,

        -- Temperature bias
        ROUND(
            SUM(temp_mean_bias * n_days) / NULLIF(SUM(n_days), 0),
            3
        )                                           AS temp_mean_bias,
        ROUND(
            SUM(temp_mae * n_days) / NULLIF(SUM(n_days), 0),
            3
        )                                           AS temp_mae,
        ROUND(
            SUM(temp_rmse * n_days) / NULLIF(SUM(n_days), 0),
            3
        )                                           AS temp_rmse,

        -- Rain bias
        ROUND(
            SUM(rain_false_positive_pct * n_days) / NULLIF(SUM(n_days), 0),
            2
        )                                           AS rain_false_positive_pct,
        ROUND(
            SUM(rain_false_negative_pct * n_days) / NULLIF(SUM(n_days), 0),
            2
        )                                           AS rain_false_negative_pct,

        -- Summary
        CASE
            WHEN SUM(temp_mean_bias * n_days) / NULLIF(SUM(n_days), 0) > 0.5
            THEN 'Overestimates'
            WHEN SUM(temp_mean_bias * n_days) / NULLIF(SUM(n_days), 0) < -0.5
            THEN 'Underestimates'
            ELSE 'Unbiased'
        END                                         AS temp_bias_direction

    FROM with_geo
    GROUP BY city, state, latitude, longitude, au_season
)

SELECT
    *,
    -- Worst-case rain miss flag for risk reporting
    rain_false_negative_pct > 20    AS high_rain_miss_rate,
    temp_mae > 3.0                  AS high_temp_error
FROM city_season
ORDER BY ABS(temp_mean_bias) DESC, city, au_season
