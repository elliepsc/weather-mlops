-- Single-row KPI summary for the Power BI homepage card visuals.
-- Combines the latest city snapshot with the most recent monthly overview.

WITH latest_date AS (
    SELECT MAX(latest_date) AS latest_date
    FROM {{ ref('mart_current_weather_snapshot') }}
),

current_snapshot AS (
    SELECT s.*
    FROM {{ ref('mart_current_weather_snapshot') }} s
    INNER JOIN latest_date l ON s.latest_date = l.latest_date
),

latest_month AS (
    SELECT MAX(year_month) AS year_month
    FROM {{ ref('bi_dashboard_overview') }}
),

overview AS (
    SELECT o.*
    FROM {{ ref('bi_dashboard_overview') }} o
    INNER JOIN latest_month lm ON o.year_month = lm.year_month
)

SELECT
    l.latest_date,
    COUNT(DISTINCT c.city)              AS current_n_cities,
    AVG(c.max_temp_tomorrow)            AS avg_max_temp_tomorrow,
    AVG(c.rain_tomorrow_proba)          AS avg_rain_tomorrow_proba,
    AVG(c.comfort_score)                AS avg_current_comfort_score,
    AVG(c.heatwave_risk)                AS avg_current_heatwave_risk,
    AVG(c.frost_risk)                   AS avg_current_frost_risk,
    AVG(c.storm_probability)            AS avg_current_storm_probability,
    o.rain_accuracy                     AS latest_month_rain_accuracy,
    o.temp_mae                          AS latest_month_temp_mae,
    o.avg_completeness_pct              AS latest_month_completeness_pct,
    o.n_critical_alerts                 AS latest_month_critical_alerts,
    o.global_status                     AS latest_global_status
FROM latest_date l
CROSS JOIN current_snapshot c
CROSS JOIN overview o
GROUP BY
    l.latest_date,
    o.rain_accuracy,
    o.temp_mae,
    o.avg_completeness_pct,
    o.n_critical_alerts,
    o.global_status
