-- Answers: "Do retrains triggered after a data gap produce less stable models
-- than normally-scheduled retrains?"
--
-- Joins retrain events with the completeness of the 7 days preceding each
-- retrain, then compares model performance before and after via LEAD().

WITH retrain AS (
    SELECT * FROM {{ ref('int_retrain_events') }}
),

completeness AS (
    SELECT * FROM {{ ref('int_data_completeness_daily') }}
),

-- For each retrain, summarise data quality in the 7 days before it
pre_retrain_quality AS (
    SELECT
        r.retrain_date,
        MAX(CASE WHEN c.has_gap THEN 1 ELSE 0 END)     AS preceded_by_gap,
        SUM(CASE WHEN c.has_gap THEN 1 ELSE 0 END)     AS gap_days_in_7d_window,
        MAX(c.missing_city_count)                       AS max_missing_cities_7d,
        AVG(c.completeness_pct)                         AS avg_completeness_7d
    FROM retrain r
    LEFT JOIN completeness c
        ON  c.date >= {{ dbt.dateadd('day', -7, 'r.retrain_date') }}
        AND c.date <  r.retrain_date
    GROUP BY r.retrain_date
)

SELECT
    r.retrain_date,
    r.source,
    r.trigger_action,
    r.trigger_reason,
    r.n_drifted_features,
    r.heavy_drift,
    r.low_accuracy,

    -- Data quality context leading up to this retrain
    q.preceded_by_gap,
    q.gap_days_in_7d_window,
    q.max_missing_cities_7d,
    q.avg_completeness_7d,

    -- Metrics at retrain time
    r.rain_accuracy_30d,
    r.temp_mae_30d,

    -- Next retrain for before/after comparison
    LEAD(r.retrain_date)        OVER (ORDER BY r.retrain_date) AS next_retrain_date,
    LEAD(r.rain_accuracy_30d)   OVER (ORDER BY r.retrain_date) AS next_rain_accuracy,
    LEAD(r.temp_mae_30d)        OVER (ORDER BY r.retrain_date) AS next_temp_mae

FROM retrain r
LEFT JOIN pre_retrain_quality q USING (retrain_date)
