-- Geographic hierarchy: country → region → state → city.
-- dim_cities only contains city + state derived from stg_weather_raw.
-- When France or Spain cities are added, state alone is ambiguous
-- ("Île-de-France" vs "SA") and hemisphere flips season logic.
--
-- This model is the single source of truth for all geographic metadata.
-- dim_cities can be deprecated in favour of this once multi-country lands.
--
-- hemisphere controls season assignment in dim_date (Southern → AU seasons,
-- Northern → EU seasons). climate_zone uses a simplified Köppen proxy
-- derived from observed temperature and rainfall ranges computed in
-- int_weather_features_stats — no hardcoding required.
--
-- NOTE: country_code follows ISO 3166-1 alpha-2.

WITH city_base AS (
    -- Pull from the existing dim_cities derivation
    SELECT
        city,
        MAX(state)          AS state,
        MAX(latitude)       AS latitude,
        MAX(longitude)      AS longitude,
        MIN(date)           AS first_observed_date,
        MAX(date)           AS last_observed_date,
        COUNT(*)            AS total_records
    FROM {{ ref('stg_weather_raw') }}
    GROUP BY city
),

-- Climate proxy: use monthly stats to infer simplified Köppen group
climate_proxy AS (
    SELECT
        city,
        AVG(avg_max_temp)           AS annual_avg_max_temp,
        AVG(avg_min_temp)           AS annual_avg_min_temp,
        SUM(n_rainy_days)           AS annual_rainy_days,
        AVG(avg_rainfall)           AS avg_monthly_rainfall_mm,
        MAX(avg_max_temp)           AS warmest_month_avg,
        MIN(avg_min_temp)           AS coldest_month_avg
    FROM {{ ref('int_weather_features_stats') }}
    GROUP BY city
),

classified AS (
    SELECT
        cb.city,
        cb.state,
        cb.latitude,
        cb.longitude,
        cb.first_observed_date,
        cb.last_observed_date,
        cb.total_records,
        cp.annual_avg_max_temp,
        cp.annual_rainy_days,

        -- Country / region: currently all AU; extend here when FR/ES arrive
        -- Pattern: add a seeds/ CSV (city, country_code, region) and LEFT JOIN
        'AU'                        AS country_code,
        'Australia'                 AS country_name,
        'Oceania'                   AS continent,

        -- Hemisphere drives season polarity in dim_date
        CASE
            WHEN cb.latitude < 0 THEN 'Southern'
            ELSE 'Northern'
        END                         AS hemisphere,

        -- Simplified Köppen proxy (data-driven, not hardcoded per city)
        -- Extend with more nuanced thresholds as FR/ES data arrives
        CASE
            WHEN cp.warmest_month_avg >= 18
             AND cp.coldest_month_avg >= 18             THEN 'Tropical (A)'
            WHEN cp.annual_rainy_days < 90
             AND cp.annual_avg_max_temp > 18            THEN 'Arid (B)'
            WHEN cp.coldest_month_avg >= 0
             AND cp.coldest_month_avg < 18              THEN 'Temperate (C)'
            WHEN cp.coldest_month_avg < 0              THEN 'Continental (D)'
            ELSE 'Unknown'
        END                         AS koppen_group,

        -- Coastal proxy: cities within ~100km of coast tend to have
        -- smaller diurnal temp range. Rough heuristic — override via seed.
        CASE
            WHEN ABS(cp.warmest_month_avg - cp.annual_avg_max_temp) < 6
            THEN 'Coastal'
            ELSE 'Inland'
        END                         AS location_type

    FROM city_base cb
    LEFT JOIN climate_proxy cp USING (city)
)

SELECT *
FROM classified
ORDER BY country_code, state, city
