-- Latest available prediction row per city (one row per city).

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, state
            ORDER BY weather_date DESC, predicted_at DESC NULLS LAST
        ) AS rn
    FROM {{ ref('mart_weather_daily_clean') }}
)

SELECT
    weather_date            AS latest_date,
    city,
    state,
    latitude,
    longitude,
    min_temp,
    max_temp,
    rainfall_clean          AS rainfall,
    rain_today,
    rain_tomorrow,
    rain_tomorrow_proba,
    max_temp_tomorrow,
    weather_type_tomorrow,
    comfort_score,
    heatwave_risk,
    frost_risk,
    storm_probability,
    predicted_at
FROM ranked
WHERE rn = 1
