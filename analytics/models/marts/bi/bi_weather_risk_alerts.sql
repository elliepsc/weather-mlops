-- Power BI alias of mart_weather_risk_alerts with year_month added.

SELECT
    weather_date,
    date_trunc('month', weather_date)   AS year_month,
    city,
    state,
    latitude,
    longitude,
    alert_type,
    alert_score,
    alert_level,
    rain_tomorrow_proba,
    heatwave_risk,
    frost_risk,
    storm_probability,
    comfort_score
FROM {{ ref('mart_weather_risk_alerts') }}
