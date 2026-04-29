-- Enriches retrain events with the monitoring context that preceded them
-- and the model metrics recorded at retrain time.

SELECT
    r.retrain_date,
    r.written_at,
    r.source,

    -- Metrics at retrain time (from monitoring history)
    m.rain_accuracy_30d,
    m.temp_mae_30d,
    m.status                            AS metrics_status,

    -- Monitoring decision that triggered (or coincided with) the retrain
    d.action                            AS trigger_action,
    d.reason                            AS trigger_reason,
    d.n_drifted_features,
    d.heavy_drift,
    d.mild_drift,
    d.low_accuracy,
    d.high_mae

FROM {{ ref('stg_retrain_events') }} r
LEFT JOIN {{ ref('stg_model_metrics_history') }} m
    ON m.date = r.retrain_date
LEFT JOIN {{ ref('stg_monitoring_decisions') }} d
    ON d.date = r.retrain_date
