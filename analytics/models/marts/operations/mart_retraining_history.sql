-- Audit trail of all retrain events with before/after performance deltas.
-- Also surfaces whether each retrain was preceded by a data gap —
-- the key question for validating the backfill → retrain architecture.

SELECT
    g.retrain_date,
    g.source,
    g.trigger_action,
    g.trigger_reason,
    g.n_drifted_features,
    g.heavy_drift,
    g.low_accuracy,

    -- Data quality in the 7 days before this retrain
    g.preceded_by_gap,
    g.gap_days_in_7d_window,
    g.max_missing_cities_7d,
    ROUND(g.avg_completeness_7d, 1)                 AS avg_completeness_7d_pct,

    -- Model metrics at retrain time
    ROUND(g.rain_accuracy_30d, 4)                   AS rain_accuracy_at_retrain,
    ROUND(g.temp_mae_30d, 3)                        AS temp_mae_at_retrain,

    -- Next retrain (for before/after comparison)
    g.next_retrain_date,
    ROUND(g.next_rain_accuracy, 4)                  AS next_rain_accuracy,
    ROUND(g.next_temp_mae, 3)                       AS next_temp_mae,

    -- Performance delta (positive = improved)
    ROUND(g.next_rain_accuracy - g.rain_accuracy_30d, 4)    AS accuracy_delta,
    ROUND(g.temp_mae_30d - g.next_temp_mae, 3)              AS mae_delta,

    -- Stability flag: did accuracy drop after this retrain?
    CASE
        WHEN g.next_rain_accuracy IS NOT NULL
         AND g.next_rain_accuracy < g.rain_accuracy_30d - 0.02
        THEN TRUE
        ELSE FALSE
    END                                             AS accuracy_degraded_after_retrain

FROM {{ ref('int_gap_to_retrain_impact') }} g
ORDER BY g.retrain_date DESC
