-- Per-city data freshness: lag between last observed date and today.
-- int_data_completeness_daily tracks GLOBAL missing city counts but does NOT
-- tell you WHICH cities are stale or for how long.
-- A city absent from the last 3 days may look fine in completeness_pct if
-- total_cities is 49/50 — but that city's predictions are stale.
--
-- Grain: one row per city.

WITH latest_per_city AS (
    SELECT
        city,
        MAX(date)   AS last_observed_date,
        MIN(date)   AS first_observed_date,
        COUNT(*)    AS total_days,
        -- Detect gaps: expected = date range length
        DATEDIFF('day', MIN(date), MAX(date)) + 1   AS date_range_days
    FROM {{ ref('stg_weather_raw') }}
    GROUP BY city
),

enriched AS (
    SELECT
        l.*,
        dc.state,
        dc.latitude,
        dc.longitude,

        -- Freshness lag
        DATEDIFF('day', l.last_observed_date, CURRENT_DATE) AS days_since_last_update,

        -- Internal completeness: what % of the date range has data?
        ROUND(100.0 * l.total_days / NULLIF(l.date_range_days, 0), 2)
            AS internal_completeness_pct,

        -- Missing days within the range (gaps, not just recent staleness)
        l.date_range_days - l.total_days    AS internal_gap_days,

        -- Freshness tier
        CASE
            WHEN DATEDIFF('day', l.last_observed_date, CURRENT_DATE) = 0
            THEN 'Current'
            WHEN DATEDIFF('day', l.last_observed_date, CURRENT_DATE) <= 1
            THEN 'Yesterday'
            WHEN DATEDIFF('day', l.last_observed_date, CURRENT_DATE) <= 3
            THEN 'Stale (2-3d)'
            WHEN DATEDIFF('day', l.last_observed_date, CURRENT_DATE) <= 7
            THEN 'Stale (4-7d)'
            ELSE 'Critical (>7d)'
        END                                 AS freshness_tier,

        -- Flag for alerting
        DATEDIFF('day', l.last_observed_date, CURRENT_DATE) > 3
            AS is_stale

    FROM latest_per_city l
    LEFT JOIN {{ ref('dim_cities') }} dc USING (city)
)

SELECT *
FROM enriched
ORDER BY days_since_last_update DESC, city
