WITH source AS (
    SELECT * FROM {{ source('raw', 'src_weather_raw') }}
)

SELECT
    CAST(date AS DATE)                      AS date,
    city,
    state,
    CAST(latitude AS DOUBLE)                AS latitude,
    CAST(longitude AS DOUBLE)               AS longitude,
    CAST(min_temp AS DOUBLE)                AS min_temp,
    CAST(max_temp AS DOUBLE)                AS max_temp,
    CAST(rainfall AS DOUBLE)                AS rainfall,
    CAST(rain_sum AS DOUBLE)                AS rain_sum,
    CAST(precipitation_hours AS DOUBLE)     AS precipitation_hours,
    CAST(evaporation AS DOUBLE)             AS evaporation,
    CAST(sunshine_hours AS DOUBLE)          AS sunshine_hours,
    wind_gust_dir,
    CAST(wind_gust_speed AS DOUBLE)         AS wind_gust_speed,
    wind_dir_9am,
    wind_dir_3pm,
    CAST(wind_speed_9am AS DOUBLE)          AS wind_speed_9am,
    CAST(wind_speed_3pm AS DOUBLE)          AS wind_speed_3pm,
    CAST(humidity_9am AS DOUBLE)            AS humidity_9am,
    CAST(humidity_3pm AS DOUBLE)            AS humidity_3pm,
    CAST(dew_point_9am AS DOUBLE)           AS dew_point_9am,
    CAST(dew_point_3pm AS DOUBLE)           AS dew_point_3pm,
    CAST(pressure_9am AS DOUBLE)            AS pressure_9am,
    CAST(pressure_3pm AS DOUBLE)            AS pressure_3pm,
    CAST(surface_pressure_9am AS DOUBLE)    AS surface_pressure_9am,
    CAST(surface_pressure_3pm AS DOUBLE)    AS surface_pressure_3pm,
    CAST(cloud_9am AS DOUBLE)               AS cloud_9am,
    CAST(cloud_3pm AS DOUBLE)               AS cloud_3pm,
    CAST(temp_9am AS DOUBLE)                AS temp_9am,
    CAST(temp_3pm AS DOUBLE)                AS temp_3pm,
    -- Stored as INTEGER in SQLite; cast to BOOLEAN for clarity
    CASE WHEN CAST(rain_today AS INTEGER) = 1 THEN TRUE ELSE FALSE END  AS rain_today,
    CAST(weather_code AS INTEGER)           AS weather_code,
    CAST(shortwave_radiation_sum AS DOUBLE) AS shortwave_radiation_sum,
    CAST(vpd_9am AS DOUBLE)                 AS vpd_9am,
    CAST(vpd_3pm AS DOUBLE)                 AS vpd_3pm,
    CAST(wind_speed_100m_9am AS DOUBLE)     AS wind_speed_100m_9am,
    CAST(wind_speed_100m_3pm AS DOUBLE)     AS wind_speed_100m_3pm

FROM source
WHERE date IS NOT NULL
  AND city IS NOT NULL
