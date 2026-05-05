-- Climatological normals per (month, city): long-run averages across all years.
-- normal_monthly_rainfall = average total rainfall for that calendar month.

SELECT
    month,
    city,
    state,
    AVG(latitude)                                               AS latitude,
    AVG(longitude)                                              AS longitude,
    COUNT(*)                                                    AS n_observations,
    COUNT(DISTINCT year)                                        AS n_years,

    AVG(min_temp)                                               AS normal_min_temp,
    AVG(max_temp)                                               AS normal_max_temp,
    AVG(rainfall_clean)                                         AS normal_daily_rainfall,
    SUM(rainfall_clean) / NULLIF(COUNT(DISTINCT year), 0)       AS normal_monthly_rainfall,
    AVG(humidity_3pm)                                           AS normal_humidity_3pm,
    AVG(wind_speed_3pm)                                         AS normal_wind_speed_3pm,
    AVG(comfort_score)                                          AS normal_comfort_score,
    AVG(heatwave_risk)                                          AS normal_heatwave_risk,
    AVG(frost_risk)                                             AS normal_frost_risk,
    AVG(storm_probability)                                      AS normal_storm_probability
FROM {{ ref('mart_weather_daily_clean') }}
GROUP BY month, city, state
