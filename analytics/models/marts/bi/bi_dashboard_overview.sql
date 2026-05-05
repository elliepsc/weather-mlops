-- Monthly global overview: aggregates weather, forecast accuracy, alerts and quality
-- into one row per year_month. Powers the Power BI homepage trend charts.

WITH weather AS (
    SELECT
        year_month,
        COUNT(DISTINCT city)            AS n_cities,
        SUM(n_days)                     AS n_city_days,
        AVG(avg_max_temp)               AS avg_max_temp,
        AVG(avg_min_temp)               AS avg_min_temp,
        SUM(total_rainfall)             AS total_rainfall,
        AVG(rainy_day_rate)             AS rainy_day_rate,
        AVG(avg_comfort_score)          AS avg_comfort_score,
        AVG(avg_heatwave_risk)          AS avg_heatwave_risk,
        AVG(avg_frost_risk)             AS avg_frost_risk,
        AVG(avg_storm_probability)      AS avg_storm_probability
    FROM {{ ref('mart_weather_monthly_city') }}
    GROUP BY year_month
),

forecast AS (
    SELECT
        year_month,
        AVG(rain_accuracy)              AS rain_accuracy,
        AVG(temp_mae)                   AS temp_mae,
        MAX(temp_max_error)             AS temp_max_error,
        SUM(n_predictions)              AS n_predictions
    FROM {{ ref('mart_forecast_accuracy_monthly') }}
    GROUP BY year_month
),

alerts AS (
    SELECT
        date_trunc('month', weather_date)                                   AS year_month,
        COUNT(*)                                                            AS n_alerts,
        SUM(CASE WHEN alert_level = 'Critical' THEN 1 ELSE 0 END)          AS n_critical_alerts,
        SUM(CASE WHEN alert_type  = 'Rain risk'     THEN 1 ELSE 0 END)     AS n_rain_alerts,
        SUM(CASE WHEN alert_type  = 'Storm risk'    THEN 1 ELSE 0 END)     AS n_storm_alerts,
        SUM(CASE WHEN alert_type  = 'Heatwave risk' THEN 1 ELSE 0 END)     AS n_heatwave_alerts,
        SUM(CASE WHEN alert_type  = 'Frost risk'    THEN 1 ELSE 0 END)     AS n_frost_alerts,
        SUM(CASE WHEN alert_type  = 'Low comfort'   THEN 1 ELSE 0 END)     AS n_low_comfort_alerts
    FROM {{ ref('mart_weather_risk_alerts') }}
    GROUP BY date_trunc('month', weather_date)
),

quality AS (
    SELECT
        date_trunc('month', weather_date)                                   AS year_month,
        AVG(completeness_pct)                                               AS avg_completeness_pct,
        SUM(CASE WHEN data_quality_status = 'Critical' THEN 1 ELSE 0 END)  AS n_critical_quality_days
    FROM {{ ref('mart_data_quality_daily') }}
    GROUP BY date_trunc('month', weather_date)
)

SELECT
    w.year_month,
    w.n_cities,
    w.n_city_days,
    w.avg_max_temp,
    w.avg_min_temp,
    w.total_rainfall,
    w.rainy_day_rate,
    w.avg_comfort_score,
    w.avg_heatwave_risk,
    w.avg_frost_risk,
    w.avg_storm_probability,

    f.rain_accuracy,
    f.temp_mae,
    f.temp_max_error,
    f.n_predictions,

    COALESCE(a.n_alerts,              0) AS n_alerts,
    COALESCE(a.n_critical_alerts,     0) AS n_critical_alerts,
    COALESCE(a.n_rain_alerts,         0) AS n_rain_alerts,
    COALESCE(a.n_storm_alerts,        0) AS n_storm_alerts,
    COALESCE(a.n_heatwave_alerts,     0) AS n_heatwave_alerts,
    COALESCE(a.n_frost_alerts,        0) AS n_frost_alerts,
    COALESCE(a.n_low_comfort_alerts,  0) AS n_low_comfort_alerts,

    q.avg_completeness_pct,
    q.n_critical_quality_days,

    CASE
        WHEN q.avg_completeness_pct < 75
          OR COALESCE(a.n_critical_alerts, 0) > 0 THEN 'Critical'
        WHEN f.rain_accuracy < 0.70
          OR f.temp_mae > 4                        THEN 'Warning'
        ELSE 'Healthy'
    END                                 AS global_status
FROM weather w
LEFT JOIN forecast f ON f.year_month = w.year_month
LEFT JOIN alerts   a ON a.year_month = w.year_month
LEFT JOIN quality  q ON q.year_month = w.year_month
