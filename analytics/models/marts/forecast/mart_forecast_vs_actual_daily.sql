-- Compares J-day predictions with J+1 actual observations.
-- See CLAUDE.md: predictions for date X correspond to actuals on date X+1.

WITH d AS (
    -- prediction day
    SELECT * FROM {{ ref('mart_weather_daily_clean') }}
),

a AS (
    -- actual day (J+1)
    SELECT * FROM {{ ref('mart_weather_daily_clean') }}
)

SELECT
    d.weather_date                                          AS prediction_date,
    CAST({{ dbt.dateadd('day', 1, 'd.weather_date') }} AS DATE) AS actual_date,
    d.city,
    d.state,
    d.latitude,
    d.longitude,

    CASE WHEN d.rain_tomorrow = TRUE THEN 1 ELSE 0 END      AS pred_rain_tomorrow,
    d.rain_tomorrow_proba                                   AS pred_rain_proba,
    CASE WHEN a.rain_today  = TRUE THEN 1 ELSE 0 END        AS actual_rain_tomorrow,

    CASE
        WHEN a.rain_today IS NULL OR d.rain_tomorrow IS NULL THEN NULL
        WHEN d.rain_tomorrow = a.rain_today                  THEN 1
        ELSE 0
    END                                                     AS rain_prediction_correct,

    CASE
        WHEN a.rain_today IS NULL OR d.rain_tomorrow IS NULL THEN 'Unknown'
        WHEN d.rain_tomorrow = TRUE  AND a.rain_today = TRUE  THEN 'True positive'
        WHEN d.rain_tomorrow = FALSE AND a.rain_today = FALSE THEN 'True negative'
        WHEN d.rain_tomorrow = TRUE  AND a.rain_today = FALSE THEN 'False positive'
        WHEN d.rain_tomorrow = FALSE AND a.rain_today = TRUE  THEN 'False negative'
        ELSE 'Unknown'
    END                                                     AS forecast_result,

    d.max_temp_tomorrow                                     AS pred_max_temp_tomorrow,
    a.max_temp                                              AS actual_max_temp_tomorrow,

    CASE
        WHEN a.max_temp IS NULL OR d.max_temp_tomorrow IS NULL THEN NULL
        ELSE ABS(d.max_temp_tomorrow - a.max_temp)
    END                                                     AS temp_abs_error,

    CASE
        WHEN a.max_temp IS NULL OR d.max_temp_tomorrow IS NULL THEN NULL
        ELSE d.max_temp_tomorrow - a.max_temp
    END                                                     AS temp_error_signed,

    d.weather_type_tomorrow,
    d.comfort_score,
    d.heatwave_risk,
    d.frost_risk,
    d.storm_probability,
    d.predicted_at

FROM d
LEFT JOIN a
    ON  a.city         = d.city
    AND a.state        = d.state
    AND a.weather_date = CAST({{ dbt.dateadd('day', 1, 'd.weather_date') }} AS DATE)
