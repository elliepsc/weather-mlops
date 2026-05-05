-- Base enriched daily mart: one row per (date, city).
-- Adds date fields, rainfall/temperature categories, risk flags, and comfort band.
-- All downstream weather marts ref this model.

WITH base AS (
    SELECT * FROM {{ ref('int_weather_final') }}
)

SELECT
    date                                                AS weather_date,
    city,
    state,
    latitude,
    longitude,

    min_temp,
    max_temp,
    rainfall,
    rain_sum,
    precipitation_hours,
    evaporation,
    sunshine_hours,
    wind_gust_dir,
    wind_gust_speed,
    wind_dir_9am,
    wind_dir_3pm,
    wind_speed_9am,
    wind_speed_3pm,
    humidity_9am,
    humidity_3pm,
    dew_point_9am,
    dew_point_3pm,
    pressure_9am,
    pressure_3pm,
    surface_pressure_9am,
    surface_pressure_3pm,
    cloud_9am,
    cloud_3pm,
    temp_9am,
    temp_3pm,
    rain_today,
    weather_code,
    shortwave_radiation_sum,
    vpd_9am,
    vpd_3pm,
    wind_speed_100m_9am,
    wind_speed_100m_3pm,

    rain_tomorrow,
    rain_tomorrow_proba,
    max_temp_tomorrow,
    weather_type_tomorrow,
    comfort_score,
    heatwave_risk,
    frost_risk,
    storm_probability,
    predicted_at,

    -- ── Date fields ──────────────────────────────────────────────────────────
    date_trunc('month', date)                           AS year_month,
    EXTRACT(YEAR    FROM date)                          AS year,
    EXTRACT(MONTH   FROM date)                          AS month,
    EXTRACT(QUARTER FROM date)                          AS quarter,
    CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 'Summer' WHEN 1 THEN 'Summer' WHEN 2 THEN 'Summer'
        WHEN  3 THEN 'Autumn' WHEN 4 THEN 'Autumn' WHEN 5 THEN 'Autumn'
        WHEN  6 THEN 'Winter' WHEN 7 THEN 'Winter' WHEN 8 THEN 'Winter'
        WHEN  9 THEN 'Spring' WHEN 10 THEN 'Spring' WHEN 11 THEN 'Spring'
    END                                                 AS season,

    -- ── Rainfall ─────────────────────────────────────────────────────────────
    COALESCE(rainfall, rain_sum, 0)                     AS rainfall_clean,
    CASE
        WHEN rain_today = TRUE
          OR COALESCE(rainfall, rain_sum, 0) > 0 THEN 1
        ELSE 0
    END                                                 AS is_rainy_day,
    CASE
        WHEN COALESCE(rainfall, rain_sum, 0) = 0   THEN 'No rain'
        WHEN COALESCE(rainfall, rain_sum, 0) < 2   THEN 'Light rain'
        WHEN COALESCE(rainfall, rain_sum, 0) < 10  THEN 'Moderate rain'
        WHEN COALESCE(rainfall, rain_sum, 0) < 30  THEN 'Heavy rain'
        ELSE 'Extreme rain'
    END                                                 AS rainfall_category,

    -- ── Temperature ──────────────────────────────────────────────────────────
    CASE WHEN max_temp >= 35 THEN 1 ELSE 0 END          AS is_hot_day,
    CASE WHEN min_temp <= 0  THEN 1 ELSE 0 END          AS is_frost_day,
    CASE
        WHEN max_temp IS NULL THEN 'Unknown'
        WHEN max_temp < 10   THEN 'Cold'
        WHEN max_temp < 20   THEN 'Mild'
        WHEN max_temp < 30   THEN 'Warm'
        WHEN max_temp < 35   THEN 'Hot'
        ELSE 'Extreme heat'
    END                                                 AS temperature_category,

    -- ── Risk flags ───────────────────────────────────────────────────────────
    CASE WHEN storm_probability >= 0.70 THEN 1 ELSE 0 END AS is_storm_risk_day,
    CASE WHEN wind_gust_speed  >= 70    THEN 1 ELSE 0 END AS is_strong_wind_day,

    -- ── Comfort band ─────────────────────────────────────────────────────────
    CASE
        WHEN comfort_score IS NULL  THEN 'Unknown'
        WHEN comfort_score >= 90    THEN 'Excellent'
        WHEN comfort_score >= 75    THEN 'Good'
        WHEN comfort_score >= 60    THEN 'Medium'
        WHEN comfort_score >= 40    THEN 'Low'
        ELSE 'Poor'
    END                                                 AS comfort_category

FROM base
