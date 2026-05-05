-- Seasonal aggregation per (year, season, city).

SELECT
    year,
    season,
    city,
    state,
    AVG(latitude)               AS latitude,
    AVG(longitude)              AS longitude,
    COUNT(*)                    AS n_days,

    AVG(min_temp)               AS avg_min_temp,
    AVG(max_temp)               AS avg_max_temp,
    SUM(rainfall_clean)         AS total_rainfall,
    AVG(rainfall_clean)         AS avg_rainfall,
    SUM(is_rainy_day)           AS rainy_days,
    AVG(humidity_3pm)           AS avg_humidity_3pm,
    AVG(sunshine_hours)         AS avg_sunshine_hours,
    AVG(wind_speed_3pm)         AS avg_wind_speed_3pm,
    AVG(comfort_score)          AS avg_comfort_score,
    AVG(heatwave_risk)          AS avg_heatwave_risk,
    AVG(frost_risk)             AS avg_frost_risk,
    AVG(storm_probability)      AS avg_storm_probability
FROM {{ ref('mart_weather_daily_clean') }}
GROUP BY year, season, city, state
