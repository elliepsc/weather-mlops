-- Power BI alias of mart_weather_extremes with year_month added.

SELECT
    weather_date,
    date_trunc('month', weather_date)   AS year_month,
    city,
    state,
    latitude,
    longitude,
    extreme_type,
    extreme_value,
    severity_level,
    max_temp,
    min_temp,
    rainfall,
    wind_gust_speed,
    humidity_3pm,
    storm_probability,
    heatwave_risk,
    frost_risk
FROM {{ ref('mart_weather_extremes') }}
