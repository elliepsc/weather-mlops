-- Joins stg_weather_raw + stg_weather_predictions into a single flat table
-- that mirrors the weather_final.csv column space with original naming
-- (rain_tomorrow, not pred_rain_tomorrow). All downstream BI marts use this.

WITH raw AS (
    SELECT * FROM {{ ref('stg_weather_raw') }}
),

predictions AS (
    SELECT * FROM {{ ref('stg_weather_predictions') }}
)

SELECT
    r.date,
    r.city,
    r.state,
    r.latitude,
    r.longitude,
    r.min_temp,
    r.max_temp,
    r.rainfall,
    r.rain_sum,
    r.precipitation_hours,
    r.evaporation,
    r.sunshine_hours,
    r.wind_gust_dir,
    r.wind_gust_speed,
    r.wind_dir_9am,
    r.wind_dir_3pm,
    r.wind_speed_9am,
    r.wind_speed_3pm,
    r.humidity_9am,
    r.humidity_3pm,
    r.dew_point_9am,
    r.dew_point_3pm,
    r.pressure_9am,
    r.pressure_3pm,
    r.surface_pressure_9am,
    r.surface_pressure_3pm,
    r.cloud_9am,
    r.cloud_3pm,
    r.temp_9am,
    r.temp_3pm,
    r.rain_today,
    r.weather_code,
    r.shortwave_radiation_sum,
    r.vpd_9am,
    r.vpd_3pm,
    r.wind_speed_100m_9am,
    r.wind_speed_100m_3pm,
    p.pred_rain_tomorrow            AS rain_tomorrow,
    p.pred_rain_proba               AS rain_tomorrow_proba,
    p.pred_max_temp_tomorrow        AS max_temp_tomorrow,
    p.pred_weather_type_tomorrow    AS weather_type_tomorrow,
    p.comfort_score,
    p.pred_heatwave_risk            AS heatwave_risk,
    p.pred_frost_risk               AS frost_risk,
    p.pred_storm_probability        AS storm_probability,
    p.predicted_at

FROM raw r
LEFT JOIN predictions p
    ON  p.date = r.date
    AND p.city = r.city
