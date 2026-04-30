-- Aggregated drift statistics per feature across the full monitoring history.
-- stg_drift_reports exists but there is no model that answers:
--   "Which features drift most often? How severe is their drift? Is it trending?"
-- This intermediate feeds mart_feature_drift_summary and informs retraining
-- decisions: features with chronic drift may need engineering intervention,
-- not just periodic retraining.

WITH drift AS (
    SELECT
        date,
        feature_name,
        ks_stat,
        p_value,
        drifted
    FROM {{ ref('stg_drift_reports') }}
),

-- Total monitoring days as denominator
monitoring_span AS (
    SELECT
        MIN(date)   AS first_date,
        MAX(date)   AS last_date,
        COUNT(DISTINCT date) AS total_days
    FROM drift
),

per_feature AS (
    SELECT
        d.feature_name,

        COUNT(DISTINCT d.date)          AS days_monitored,
        SUM(CASE WHEN d.drifted THEN 1 ELSE 0 END) AS drift_days,
        ROUND(
            100.0 * SUM(CASE WHEN d.drifted THEN 1 ELSE 0 END)
            / NULLIF(COUNT(DISTINCT d.date), 0),
            2
        )                               AS drift_frequency_pct,

        -- KS-stat statistics (severity proxy; higher = more distributional shift)
        ROUND(AVG(d.ks_stat), 4)        AS mean_ks_stat,
        ROUND(MAX(d.ks_stat), 4)        AS max_ks_stat,
        ROUND(
            AVG(CASE WHEN d.drifted THEN d.ks_stat END),
            4
        )                               AS mean_ks_stat_when_drifted,

        -- p-value summary
        ROUND(AVG(d.p_value), 4)        AS mean_p_value,
        ROUND(MIN(d.p_value), 4)        AS min_p_value,

        -- Recent drift: last 30 days
        SUM(CASE
            WHEN d.date >= {{ dbt.dateadd('day', -30, 'current_date') }}
             AND d.drifted
            THEN 1 ELSE 0
        END)                            AS drift_days_last_30d,

        -- Trend flag: is drift accelerating recently?
        ROUND(
            100.0 * SUM(CASE
                WHEN d.date >= {{ dbt.dateadd('day', -30, 'current_date') }}
                 AND d.drifted THEN 1 ELSE 0
            END) / NULLIF(
                COUNT(DISTINCT CASE
                    WHEN d.date >= {{ dbt.dateadd('day', -30, 'current_date') }}
                    THEN d.date END
                ), 0
            ),
            2
        )                               AS drift_freq_last_30d_pct

    FROM drift d
    GROUP BY d.feature_name
),

ranked AS (
    SELECT
        *,
        -- Rank by overall drift frequency (1 = most problematic feature)
        ROW_NUMBER() OVER (ORDER BY drift_frequency_pct DESC, mean_ks_stat DESC)
            AS drift_rank,

        -- Criticality tier
        CASE
            WHEN drift_frequency_pct >= 50  THEN 'Chronic'
            WHEN drift_frequency_pct >= 20  THEN 'Frequent'
            WHEN drift_frequency_pct >= 5   THEN 'Occasional'
            ELSE 'Rare'
        END                             AS drift_tier,

        -- Accelerating flag: recent drift rate > historical rate
        drift_freq_last_30d_pct > drift_frequency_pct * 1.5
            AS drift_accelerating

    FROM per_feature
)

SELECT r.*, s.first_date, s.last_date, s.total_days AS total_monitoring_days
FROM ranked r
CROSS JOIN monitoring_span s
ORDER BY drift_rank
