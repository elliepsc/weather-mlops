-- Long format: one row per monitoring run date × feature.
-- Use PIVOT in Power BI or mart_retraining_history for wide views.

SELECT
    CAST(date AS DATE)              AS date,
    feature_name,
    CAST(ks_stat AS DOUBLE)         AS ks_stat,
    CAST(p_value AS DOUBLE)         AS p_value,
    CAST(drifted AS BOOLEAN)        AS drifted

FROM {{ source('raw', 'src_drift_reports') }}
