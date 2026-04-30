-- Per-city data freshness dashboard.
-- Answers the operational question: "Which cities are stale right now?"
-- int_data_completeness_daily tracks global missing counts but NOT which
-- cities are affected or for how long.
--
-- This mart is the primary table for a city-level freshness alert view.
-- Intended for the data engineering team, not analysts.

SELECT
    city,
    state,
    latitude,
    longitude,
    last_observed_date,
    first_observed_date,
    days_since_last_update,
    freshness_tier,
    is_stale,
    total_days,
    date_range_days,
    internal_gap_days,
    ROUND(internal_completeness_pct, 2)     AS internal_completeness_pct,

    -- SLA breach flag: alert if stale > N days (configurable via var)
    days_since_last_update > {{ var('freshness_sla_days', 3) }}
        AS sla_breached,

    -- Time since first observation (pipeline age)
    DATEDIFF('day', first_observed_date, CURRENT_DATE)
        AS pipeline_age_days

FROM {{ ref('int_data_freshness_by_city') }}
ORDER BY days_since_last_update DESC, city
