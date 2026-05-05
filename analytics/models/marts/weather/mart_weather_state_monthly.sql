-- Monthly aggregation by state (all cities aggregated).

WITH agg AS (
    SELECT
        year_month,
        year,
        month,
        state,
        COUNT(DISTINCT city)        AS n_cities,
        COUNT(*)                    AS n_city_days,

        AVG(min_temp)               AS avg_min_temp,
        AVG(max_temp)               AS avg_max_temp,
        MIN(min_temp)               AS min_temp_recorded,
        MAX(max_temp)               AS max_temp_recorded,

        SUM(rainfall_clean)         AS total_rainfall,
        AVG(rainfall_clean)         AS avg_rainfall,
        SUM(is_rainy_day)           AS rainy_days,

        AVG(humidity_3pm)           AS avg_humidity_3pm,
        AVG(wind_speed_3pm)         AS avg_wind_speed_3pm,
        MAX(wind_gust_speed)        AS max_wind_gust_speed,

        AVG(comfort_score)          AS avg_comfort_score,
        AVG(heatwave_risk)          AS avg_heatwave_risk,
        AVG(frost_risk)             AS avg_frost_risk,
        AVG(storm_probability)      AS avg_storm_probability,

        SUM(is_hot_day)             AS hot_days,
        SUM(is_frost_day)           AS frost_days,
        SUM(is_storm_risk_day)      AS storm_risk_days
    FROM {{ ref('mart_weather_daily_clean') }}
    GROUP BY year_month, year, month, state
)

SELECT
    *,
    rainy_days * 1.0 / NULLIF(n_city_days, 0)          AS rainy_day_rate
FROM agg
