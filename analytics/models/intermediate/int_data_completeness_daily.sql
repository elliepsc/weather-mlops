-- Daily data completeness: how many cities have complete data vs. expected.
-- "Expected" is the max distinct cities seen in the last 90 days, making
-- this self-calibrating when new cities are added to the pipeline.

WITH daily_counts AS (
    SELECT
        date,
        COUNT(DISTINCT city)                                    AS present_cities,
        COUNT(DISTINCT CASE
            WHEN min_temp IS NOT NULL
             AND max_temp IS NOT NULL
            THEN city
        END)                                                    AS complete_cities
    FROM {{ ref('stg_weather_raw') }}
    GROUP BY date
),

baseline AS (
    SELECT MAX(complete_cities) AS expected_cities
    FROM daily_counts
    WHERE date >= {{ dbt.dateadd('day', -90, 'current_date') }}
)

SELECT
    d.date,
    d.present_cities,
    d.complete_cities,
    b.expected_cities,
    b.expected_cities - d.complete_cities               AS missing_city_count,
    CASE
        WHEN b.expected_cities > 0
        THEN ROUND(
            100.0 * d.complete_cities / b.expected_cities,
            2
        )
        ELSE NULL
    END                                                 AS completeness_pct,
    d.complete_cities < b.expected_cities               AS has_gap

FROM daily_counts d
CROSS JOIN baseline b
