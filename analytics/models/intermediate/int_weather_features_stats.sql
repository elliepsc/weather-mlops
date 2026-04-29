-- Monthly feature statistics per city.
-- Feeds mart_forecast_vs_actual_timeline context and can validate whether
-- KS-test drift detected by the monitoring DAG is seasonally expected.

WITH monthly AS (
    SELECT
        city,
        {{ dbt.date_trunc('month', 'date') }}   AS month,
        COUNT(*)                                AS n_days,
        SUM(CASE WHEN rain_today THEN 1 ELSE 0 END) AS n_rainy_days,

        -- Temperature
        AVG(max_temp)                           AS avg_max_temp,
        STDDEV(max_temp)                        AS stddev_max_temp,
        MIN(max_temp)                           AS min_max_temp,
        MAX(max_temp)                           AS max_max_temp,
        AVG(min_temp)                           AS avg_min_temp,
        STDDEV(min_temp)                        AS stddev_min_temp,

        -- Rainfall
        AVG(rainfall)                           AS avg_rainfall,
        MAX(rainfall)                           AS max_rainfall,
        AVG(precipitation_hours)                AS avg_precipitation_hours,

        -- Humidity / pressure
        AVG(humidity_9am)                       AS avg_humidity_9am,
        AVG(humidity_3pm)                       AS avg_humidity_3pm,
        AVG(pressure_9am)                       AS avg_pressure_9am,
        AVG(pressure_3pm)                       AS avg_pressure_3pm,

        -- Wind
        AVG(wind_gust_speed)                    AS avg_wind_gust_speed,
        MAX(wind_gust_speed)                    AS max_wind_gust_speed,

        -- Cloud / radiation
        AVG(cloud_9am)                          AS avg_cloud_9am,
        AVG(cloud_3pm)                          AS avg_cloud_3pm,
        AVG(shortwave_radiation_sum)            AS avg_shortwave_radiation_sum

    FROM {{ ref('stg_weather_raw') }}
    GROUP BY city, {{ dbt.date_trunc('month', 'date') }}
)

SELECT *
FROM monthly
