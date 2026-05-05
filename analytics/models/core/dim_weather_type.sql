-- Weather type dimension derived from ML prediction output.
-- weather_type_group groups rare or NULL types under readable labels.

SELECT DISTINCT
    pred_weather_type_tomorrow          AS weather_type,
    CASE
        WHEN pred_weather_type_tomorrow = 'Sunny'   THEN 'Dry forecast'
        WHEN pred_weather_type_tomorrow = 'Cloudy'  THEN 'Cloudy forecast'
        WHEN pred_weather_type_tomorrow = 'Rainy'   THEN 'Rain forecast'
        WHEN pred_weather_type_tomorrow = 'Stormy'  THEN 'Storm forecast'
        WHEN pred_weather_type_tomorrow IS NULL      THEN 'Unknown'
        ELSE pred_weather_type_tomorrow
    END                                 AS weather_type_group
FROM {{ ref('stg_weather_predictions') }}
