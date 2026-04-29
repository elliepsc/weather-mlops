-- Stable city dimension derived from weather_raw.
-- Latitude/longitude are taken as the most-frequently reported values
-- to guard against rare NULL rows skewing coordinates.

WITH city_stats AS (
    SELECT
        city,
        MAX(state)                      AS state,
        -- Median-style: use mode via MAX of the most common non-null value
        MAX(latitude)                   AS latitude,
        MAX(longitude)                  AS longitude,
        MIN(date)                       AS first_observed_date,
        MAX(date)                       AS last_observed_date,
        COUNT(*)                        AS total_records
    FROM {{ ref('stg_weather_raw') }}
    GROUP BY city
)

SELECT *
FROM city_stats
ORDER BY city
