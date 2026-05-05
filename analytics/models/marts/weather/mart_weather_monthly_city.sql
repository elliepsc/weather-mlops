-- Monthly aggregation per city: temperature, rainfall, wind, comfort, risk scores.

WITH agg AS (
    SELECT
        year_month,
        year,
        month,
        city,
        state,
        AVG(latitude)               AS latitude,
        AVG(longitude)              AS longitude,
        COUNT(*)                    AS n_days,

        AVG(min_temp)               AS avg_min_temp,
        AVG(max_temp)               AS avg_max_temp,
        MIN(min_temp)               AS min_temp_recorded,
        MAX(max_temp)               AS max_temp_recorded,
        AVG(temp_9am)               AS avg_temp_9am,
        AVG(temp_3pm)               AS avg_temp_3pm,

        SUM(rainfall_clean)         AS total_rainfall,
        AVG(rainfall_clean)         AS avg_rainfall,
        SUM(is_rainy_day)           AS rainy_days,

        AVG(humidity_9am)           AS avg_humidity_9am,
        AVG(humidity_3pm)           AS avg_humidity_3pm,
        AVG(pressure_9am)           AS avg_pressure_9am,
        AVG(pressure_3pm)           AS avg_pressure_3pm,
        AVG(cloud_9am)              AS avg_cloud_9am,
        AVG(cloud_3pm)              AS avg_cloud_3pm,
        AVG(wind_speed_9am)         AS avg_wind_speed_9am,
        AVG(wind_speed_3pm)         AS avg_wind_speed_3pm,
        MAX(wind_gust_speed)        AS max_wind_gust_speed,
        AVG(evaporation)            AS avg_evaporation,
        AVG(sunshine_hours)         AS avg_sunshine_hours,
        AVG(shortwave_radiation_sum) AS avg_shortwave_radiation_sum,

        AVG(comfort_score)          AS avg_comfort_score,
        AVG(heatwave_risk)          AS avg_heatwave_risk,
        AVG(frost_risk)             AS avg_frost_risk,
        AVG(storm_probability)      AS avg_storm_probability,

        SUM(is_hot_day)             AS hot_days,
        SUM(is_frost_day)           AS frost_days,
        SUM(is_storm_risk_day)      AS storm_risk_days,
        SUM(is_strong_wind_day)     AS strong_wind_days
    FROM {{ ref('mart_weather_daily_clean') }}
    GROUP BY year_month, year, month, city, state
)

SELECT
    *,
    n_days - rainy_days                                 AS dry_days,
    rainy_days * 1.0 / NULLIF(n_days, 0)               AS rainy_day_rate
FROM agg
