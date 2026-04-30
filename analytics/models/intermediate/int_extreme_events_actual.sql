-- Flags actual extreme weather events from observed data.
-- Thresholds follow Australian Bureau of Meteorology (BoM) definitions:
--   Heatwave:  max_temp >= 35°C (3 consecutive days not enforced here — single-day flag)
--   Frost:     min_temp < 2°C
--   Storm:     wind_gust_speed >= 90 km/h OR weather_code IN (95,96,99)
--   Heavy rain: rainfall >= 25 mm/day
--
-- Purpose: join to int_actuals_vs_predictions to measure model performance
-- specifically when stakes are highest. The model predicts heatwave_risk,
-- frost_risk, storm_probability — but nowhere is their accuracy on actual
-- extreme days evaluated.

WITH raw AS (
    SELECT
        date,
        city,
        max_temp,
        min_temp,
        rainfall,
        wind_gust_speed,
        weather_code,
        cloud_9am,
        cloud_3pm,
        humidity_3pm,
        pressure_3pm
    FROM {{ ref('stg_weather_raw') }}
),

flagged AS (
    SELECT
        date,
        city,

        -- Primary extreme flags
        max_temp >= 35                                          AS is_heatwave_day,
        min_temp < 2                                            AS is_frost_day,
        COALESCE(wind_gust_speed >= 90, FALSE)
            OR weather_code IN (95, 96, 99)                     AS is_storm_day,
        COALESCE(rainfall >= 25, FALSE)                         AS is_heavy_rain_day,

        -- Compound extreme (2+ flags simultaneously)
        (
            (max_temp >= 35)::INTEGER
            + (min_temp < 2)::INTEGER
            + (COALESCE(wind_gust_speed >= 90, FALSE) OR weather_code IN (95,96,99))::INTEGER
            + (COALESCE(rainfall >= 25, FALSE))::INTEGER
        ) >= 2                                                  AS is_compound_extreme,

        -- Severity label for dashboarding
        CASE
            WHEN max_temp >= 40                                 THEN 'Extreme heat (≥40°C)'
            WHEN max_temp >= 35                                 THEN 'Heatwave (35-39°C)'
            WHEN min_temp < 0                                   THEN 'Hard frost (<0°C)'
            WHEN min_temp < 2                                   THEN 'Frost (0-2°C)'
            WHEN weather_code IN (95, 96, 99)                   THEN 'Thunderstorm'
            WHEN COALESCE(wind_gust_speed >= 90, FALSE)         THEN 'High wind gust'
            WHEN COALESCE(rainfall >= 25, FALSE)                THEN 'Heavy rain'
            ELSE 'Normal'
        END                                                     AS event_label,

        max_temp,
        min_temp,
        rainfall,
        wind_gust_speed,
        weather_code

    FROM raw
)

SELECT *
FROM flagged
-- Expose all rows so downstream can filter freely
