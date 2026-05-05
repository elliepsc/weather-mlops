-- Rename all prediction columns with pred_ prefix to avoid confusion with actuals
-- in downstream joins. date = the reference date; predictions are for date+1.

WITH source AS (
    SELECT * FROM {{ source('raw', 'src_weather_predictions') }}
)

SELECT
    CAST(date AS DATE)                              AS date,
    city,
    CASE WHEN CAST(rain_tomorrow AS INTEGER) = 1
         THEN TRUE ELSE FALSE END                   AS pred_rain_tomorrow,
    CAST(rain_tomorrow_proba AS DOUBLE)             AS pred_rain_proba,
    CAST(max_temp_tomorrow AS DOUBLE)               AS pred_max_temp_tomorrow,
    weather_type_tomorrow                           AS pred_weather_type_tomorrow,
    CAST(comfort_score AS DOUBLE)                   AS comfort_score,
    CAST(heatwave_risk AS DOUBLE)                   AS pred_heatwave_risk,
    CAST(frost_risk AS DOUBLE)                      AS pred_frost_risk,
    CAST(storm_probability AS DOUBLE)               AS pred_storm_probability,
    CAST(predicted_at AS TIMESTAMP)                 AS predicted_at

FROM source
WHERE date IS NOT NULL
  AND city IS NOT NULL
  AND CAST(date AS DATE) >= CAST('{{ var("min_date") }}' AS DATE)
