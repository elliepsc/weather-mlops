-- Daily ops dashboard — one row per day for the last 30 days.
-- Covers data completeness, monitoring decisions, model metrics,
-- and days since last retrain in a single queryable table.

WITH dates AS (
    SELECT DISTINCT date
    FROM {{ ref('stg_weather_raw') }}
    WHERE date >= {{ dbt.dateadd('day', -1 * var('mlops_health_days'), 'current_date') }}
),

-- Last retrain on or before each date (portable correlated-subquery replacement)
last_retrain_per_date AS (
    SELECT
        d.date,
        MAX(r.retrain_date)     AS last_retrain_date
    FROM dates d
    LEFT JOIN {{ ref('stg_retrain_events') }} r
        ON r.retrain_date <= d.date
    GROUP BY d.date
)

SELECT
    d.date,

    -- Data completeness
    c.completeness_pct,
    c.missing_city_count,
    c.present_cities,
    c.expected_cities,
    c.has_gap,

    -- Monitoring decision
    m.action                        AS monitoring_action,
    m.reason                        AS monitoring_reason,
    m.n_drifted_features,
    m.heavy_drift,
    m.mild_drift,
    m.low_accuracy,
    m.high_mae,

    -- Model metrics (from monitoring run)
    m.rain_accuracy_30d,
    m.temp_mae_30d,

    -- Retrain recency
    lr.last_retrain_date,
    {{ datediff_days('d.date', 'lr.last_retrain_date') }} AS days_since_retrain

FROM dates d
LEFT JOIN {{ ref('int_data_completeness_daily') }} c
    ON c.date = d.date
LEFT JOIN {{ ref('stg_monitoring_decisions') }} m
    ON m.date = d.date
LEFT JOIN last_retrain_per_date lr
    ON lr.date = d.date
ORDER BY d.date DESC
