-- Current snapshot enriched with binary critical flags for Power BI conditional formatting.

SELECT
    *,
    CASE WHEN rain_tomorrow_proba >= 0.80 THEN 1 ELSE 0 END AS critical_rain_flag,
    CASE WHEN storm_probability   >= 0.80 THEN 1 ELSE 0 END AS critical_storm_flag,
    CASE WHEN heatwave_risk       >= 0.80 THEN 1 ELSE 0 END AS critical_heatwave_flag,
    CASE WHEN frost_risk          >= 0.80 THEN 1 ELSE 0 END AS critical_frost_flag,
    CASE WHEN comfort_score        < 60   THEN 1 ELSE 0 END AS low_comfort_flag
FROM {{ ref('mart_current_weather_snapshot') }}
