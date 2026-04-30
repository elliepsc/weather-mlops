-- Feature drift leaderboard for MLOps dashboards and retraining decisions.
-- Answers: which features drift most? Is drift accelerating? What's the
-- monthly trend?
--
-- This mart is missing from the existing layer: stg_drift_reports is queryable
-- but no model aggregates it for decision-making or dashboarding.
-- Without this, the only drift signal available is the binary n_drifted_features
-- in stg_monitoring_decisions — which loses feature-level granularity.

WITH drift AS (
    SELECT
        d.*,
        dd.au_season,
        dd.year,
        dd.month
    FROM {{ ref('stg_drift_reports') }} d
    LEFT JOIN {{ ref('dim_date') }} dd ON dd.date = d.date
),

-- Monthly drift rate per feature
monthly_per_feature AS (
    SELECT
        feature_name,
        year,
        month,
        au_season,
        COUNT(*)                                        AS days_monitored,
        SUM(CASE WHEN drifted THEN 1 ELSE 0 END)       AS drift_days,
        ROUND(AVG(ks_stat), 4)                          AS mean_ks_stat,
        ROUND(MAX(ks_stat), 4)                          AS max_ks_stat,
        ROUND(AVG(p_value), 4)                          AS mean_p_value
    FROM drift
    GROUP BY feature_name, year, month, au_season
),

-- Overall summary from the pre-aggregated intermediate
summary AS (
    SELECT
        feature_name,
        drift_rank,
        drift_tier,
        drift_frequency_pct,
        mean_ks_stat,
        max_ks_stat,
        drift_days_last_30d,
        drift_freq_last_30d_pct,
        drift_accelerating,
        total_monitoring_days,
        first_date,
        last_date
    FROM {{ ref('int_feature_drift_ranking') }}
)

SELECT
    s.drift_rank,
    s.drift_tier,
    m.feature_name,
    m.year,
    m.month,
    m.au_season,
    m.days_monitored,
    m.drift_days,
    -- Monthly drift rate for time-series chart
    ROUND(100.0 * m.drift_days / NULLIF(m.days_monitored, 0), 2) AS monthly_drift_pct,
    m.mean_ks_stat,
    m.max_ks_stat,
    m.mean_p_value,

    -- Global context from ranking
    s.drift_frequency_pct                           AS overall_drift_pct,
    s.drift_freq_last_30d_pct                       AS recent_drift_pct,
    s.drift_accelerating,
    s.total_monitoring_days

FROM monthly_per_feature m
LEFT JOIN summary s USING (feature_name)
ORDER BY s.drift_rank, m.year DESC, m.month DESC
