-- Extreme weather events: one row per (date, city, extreme_type).
-- Thresholds: heat>=35, cold<=2, heavy_rain>=20, strong_wind>=60

SELECT
    weather_date, city, state, latitude, longitude,
    'Extreme heat'          AS extreme_type,
    max_temp                AS extreme_value,
    CASE
        WHEN max_temp >= 45 THEN 'Critical'
        WHEN max_temp >= 40 THEN 'High'
        ELSE 'Medium'
    END                     AS severity_level,
    max_temp, min_temp, rainfall_clean AS rainfall,
    wind_gust_speed, humidity_3pm, storm_probability, heatwave_risk, frost_risk
FROM {{ ref('mart_weather_daily_clean') }}
WHERE max_temp >= 35

UNION ALL

SELECT
    weather_date, city, state, latitude, longitude,
    'Extreme cold'          AS extreme_type,
    min_temp                AS extreme_value,
    CASE
        WHEN min_temp <= -5 THEN 'Critical'
        WHEN min_temp <= 0  THEN 'High'
        ELSE 'Medium'
    END                     AS severity_level,
    max_temp, min_temp, rainfall_clean AS rainfall,
    wind_gust_speed, humidity_3pm, storm_probability, heatwave_risk, frost_risk
FROM {{ ref('mart_weather_daily_clean') }}
WHERE min_temp <= 2

UNION ALL

SELECT
    weather_date, city, state, latitude, longitude,
    'Heavy rain'            AS extreme_type,
    rainfall_clean          AS extreme_value,
    CASE
        WHEN rainfall_clean >= 50 THEN 'Critical'
        WHEN rainfall_clean >= 30 THEN 'High'
        ELSE 'Medium'
    END                     AS severity_level,
    max_temp, min_temp, rainfall_clean AS rainfall,
    wind_gust_speed, humidity_3pm, storm_probability, heatwave_risk, frost_risk
FROM {{ ref('mart_weather_daily_clean') }}
WHERE rainfall_clean >= 20

UNION ALL

SELECT
    weather_date, city, state, latitude, longitude,
    'Strong wind'           AS extreme_type,
    wind_gust_speed         AS extreme_value,
    CASE
        WHEN wind_gust_speed >= 90 THEN 'Critical'
        WHEN wind_gust_speed >= 70 THEN 'High'
        ELSE 'Medium'
    END                     AS severity_level,
    max_temp, min_temp, rainfall_clean AS rainfall,
    wind_gust_speed, humidity_3pm, storm_probability, heatwave_risk, frost_risk
FROM {{ ref('mart_weather_daily_clean') }}
WHERE wind_gust_speed >= 60
