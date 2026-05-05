-- Global scorecard per (year_month, city): merges weather quality, forecast accuracy,
-- and data completeness into a single composite score (0-100 scale).

WITH weather AS (
    SELECT
        year_month,
        city,
        state,
        AVG(latitude)               AS latitude,
        AVG(longitude)              AS longitude,
        AVG(avg_max_temp)           AS avg_max_temp,
        AVG(avg_min_temp)           AS avg_min_temp,
        SUM(total_rainfall)         AS total_rainfall,
        AVG(rainy_day_rate)         AS rainy_day_rate,
        AVG(avg_comfort_score)      AS avg_comfort_score,
        AVG(avg_heatwave_risk)      AS avg_heatwave_risk,
        AVG(avg_frost_risk)         AS avg_frost_risk,
        AVG(avg_storm_probability)  AS avg_storm_probability
    FROM {{ ref('mart_weather_monthly_city') }}
    GROUP BY year_month, city, state
),

forecast AS (
    SELECT
        year_month,
        city,
        state,
        AVG(rain_accuracy)          AS forecast_rain_accuracy,
        AVG(temp_mae)               AS forecast_temp_mae
    FROM {{ ref('mart_forecast_accuracy_monthly') }}
    GROUP BY year_month, city, state
),

quality AS (
    SELECT
        date_trunc('month', weather_date)   AS year_month,
        city,
        state,
        AVG(completeness_pct)               AS data_completeness_pct
    FROM {{ ref('mart_data_quality_daily') }}
    GROUP BY date_trunc('month', weather_date), city, state
),

scored AS (
    SELECT
        w.*,
        f.forecast_rain_accuracy,
        f.forecast_temp_mae,
        q.data_completeness_pct,

        ROUND(
            COALESCE(w.avg_comfort_score, 0) * 0.30
            + (1 - COALESCE(w.avg_heatwave_risk,    0)) * 15
            + (1 - COALESCE(w.avg_frost_risk,        0)) * 10
            + (1 - COALESCE(w.avg_storm_probability, 0)) * 15
            + COALESCE(f.forecast_rain_accuracy, 0.5)    * 20
            + (1 - LEAST(COALESCE(f.forecast_temp_mae, 10) / 10.0, 1)) * 10
            + COALESCE(q.data_completeness_pct, 0)       * 0.10
        , 2)                                AS global_weather_score
    FROM weather w
    LEFT JOIN forecast f
        ON  f.year_month = w.year_month AND f.city = w.city AND f.state = w.state
    LEFT JOIN quality q
        ON  q.year_month = w.year_month AND q.city = w.city AND q.state = w.state
)

SELECT
    *,
    RANK() OVER (PARTITION BY year_month ORDER BY global_weather_score DESC) AS city_rank,
    CASE
        WHEN global_weather_score >= 85 THEN 'Excellent'
        WHEN global_weather_score >= 70 THEN 'Good'
        WHEN global_weather_score >= 55 THEN 'Medium'
        WHEN global_weather_score >= 40 THEN 'To monitor'
        ELSE 'Critical'
    END                                     AS performance_category
FROM scored
