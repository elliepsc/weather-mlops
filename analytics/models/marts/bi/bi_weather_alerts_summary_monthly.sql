-- Monthly alert summary by (year_month, state, alert_type, alert_level).
-- Powers bar/stacked charts and alert timelines in Power BI.

SELECT
    date_trunc('month', weather_date)   AS year_month,
    state,
    alert_type,
    alert_level,
    COUNT(*)                            AS n_alerts,
    COUNT(DISTINCT city)                AS n_cities_impacted,
    AVG(alert_score)                    AS avg_alert_score,
    MAX(alert_score)                    AS max_alert_score
FROM {{ ref('mart_weather_risk_alerts') }}
GROUP BY date_trunc('month', weather_date), state, alert_type, alert_level
