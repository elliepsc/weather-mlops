-- One row per (date, city, alert_type) for every day where a risk score >= 0.40.
-- alert_level: Medium >= 0.40 | High >= 0.60 | Critical >= 0.80

WITH base AS (
    SELECT
        weather_date,
        city,
        state,
        latitude,
        longitude,
        rain_tomorrow_proba,
        heatwave_risk,
        frost_risk,
        storm_probability,
        comfort_score
    FROM {{ ref('mart_weather_daily_clean') }}
),

alerts AS (
    SELECT
        weather_date, city, state, latitude, longitude,
        'Rain risk'             AS alert_type,
        rain_tomorrow_proba     AS alert_score,
        rain_tomorrow_proba, heatwave_risk, frost_risk, storm_probability, comfort_score
    FROM base
    WHERE rain_tomorrow_proba >= 0.40

    UNION ALL

    SELECT
        weather_date, city, state, latitude, longitude,
        'Storm risk'            AS alert_type,
        storm_probability       AS alert_score,
        rain_tomorrow_proba, heatwave_risk, frost_risk, storm_probability, comfort_score
    FROM base
    WHERE storm_probability >= 0.40

    UNION ALL

    SELECT
        weather_date, city, state, latitude, longitude,
        'Heatwave risk'         AS alert_type,
        heatwave_risk           AS alert_score,
        rain_tomorrow_proba, heatwave_risk, frost_risk, storm_probability, comfort_score
    FROM base
    WHERE heatwave_risk >= 0.40

    UNION ALL

    SELECT
        weather_date, city, state, latitude, longitude,
        'Frost risk'            AS alert_type,
        frost_risk              AS alert_score,
        rain_tomorrow_proba, heatwave_risk, frost_risk, storm_probability, comfort_score
    FROM base
    WHERE frost_risk >= 0.40

    UNION ALL

    SELECT
        weather_date, city, state, latitude, longitude,
        'Low comfort'                   AS alert_type,
        1 - (comfort_score / 100.0)     AS alert_score,
        rain_tomorrow_proba, heatwave_risk, frost_risk, storm_probability, comfort_score
    FROM base
    WHERE comfort_score IS NOT NULL AND comfort_score < 60
)

SELECT
    *,
    CASE
        WHEN alert_score >= 0.80 THEN 'Critical'
        WHEN alert_score >= 0.60 THEN 'High'
        ELSE 'Medium'
    END                                                 AS alert_level
FROM alerts
