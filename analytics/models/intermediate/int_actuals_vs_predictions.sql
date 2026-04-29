-- Critical join: predictions for date X correspond to actuals on date X+1.
-- See CLAUDE.md: "weather_predictions.max_temp_tomorrow must be compared
-- with next-day actual weather_raw.max_temp, not same-day."

WITH predictions AS (
    SELECT * FROM {{ ref('stg_weather_predictions') }}
),

raw AS (
    SELECT * FROM {{ ref('stg_weather_raw') }}
)

SELECT
    p.date                                              AS prediction_date,
    p.city,
    -- Actuals live one day ahead of the prediction reference date
    {{ dbt.dateadd('day', 1, 'p.date') }}               AS actual_date,

    -- Predictions
    p.pred_rain_tomorrow,
    p.pred_rain_proba,
    p.pred_max_temp_tomorrow,
    p.pred_weather_type_tomorrow,
    p.pred_heatwave_risk,
    p.pred_frost_risk,
    p.pred_storm_probability,
    p.comfort_score,
    p.predicted_at,

    -- Actuals (from next day's raw observation)
    a.rain_today                                        AS actual_rain,
    a.max_temp                                          AS actual_max_temp,
    a.min_temp                                          AS actual_min_temp,
    a.rainfall                                          AS actual_rainfall,
    a.weather_code                                      AS actual_weather_code,

    -- Pre-computed performance flags for downstream aggregation
    CASE
        WHEN p.pred_rain_tomorrow = a.rain_today THEN 1
        ELSE 0
    END                                                 AS rain_correct,
    CASE
        WHEN p.pred_rain_tomorrow IS NOT NULL
         AND a.rain_today IS NOT NULL
        THEN ABS(
            CAST(p.pred_rain_tomorrow AS INTEGER)
            - CAST(a.rain_today AS INTEGER)
        )
        ELSE NULL
    END                                                 AS rain_abs_error,
    ABS(p.pred_max_temp_tomorrow - a.max_temp)          AS temp_abs_error,

    -- Flag rows where actuals are available (join succeeded)
    a.max_temp IS NOT NULL                              AS has_actuals

FROM predictions p
LEFT JOIN raw a
    ON  a.city = p.city
    AND a.date = {{ dbt.dateadd('day', 1, 'p.date') }}
