-- Comfort segmentation: city-days grouped by (year_month, state, comfort_category,
-- temperature_category, rainfall_category). Powers scatter/heatmap visuals in Power BI.

SELECT
    year_month,
    state,
    comfort_category,
    temperature_category,
    rainfall_category,
    COUNT(*)                    AS n_city_days,
    COUNT(DISTINCT city)        AS n_cities,
    AVG(comfort_score)          AS avg_comfort_score,
    AVG(max_temp)               AS avg_max_temp,
    AVG(rainfall_clean)         AS avg_rainfall,
    AVG(humidity_3pm)           AS avg_humidity_3pm,
    AVG(storm_probability)      AS avg_storm_probability,
    AVG(heatwave_risk)          AS avg_heatwave_risk,
    AVG(frost_risk)             AS avg_frost_risk
FROM {{ ref('mart_weather_daily_clean') }}
GROUP BY year_month, state, comfort_category, temperature_category, rainfall_category
