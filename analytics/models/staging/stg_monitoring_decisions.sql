SELECT
    CAST(date AS DATE)              AS date,
    action,
    reason,
    CAST(n_drifted AS INTEGER)      AS n_drifted_features,
    -- drifted_features is stored as a JSON string list
    drifted_features,
    CAST(low_accuracy AS BOOLEAN)   AS low_accuracy,
    CAST(high_mae AS BOOLEAN)       AS high_mae,
    CAST(heavy_drift AS BOOLEAN)    AS heavy_drift,
    CAST(mild_drift AS BOOLEAN)     AS mild_drift,
    CAST(rain_accuracy_30d AS DOUBLE) AS rain_accuracy_30d,
    CAST(temp_mae_30d AS DOUBLE)    AS temp_mae_30d

FROM {{ source('raw', 'src_monitoring_decisions') }}
