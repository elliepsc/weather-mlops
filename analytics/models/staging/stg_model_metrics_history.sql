-- Deduplicate by date, keeping the last recorded run (highest row_num).
-- Multiple rows per date arise from intraday re-runs of the monitoring DAG.

WITH source AS (
    SELECT * FROM {{ source('raw', 'src_model_metrics_history') }}
),

deduped AS (
    SELECT
        CAST(date AS DATE)              AS date,
        status,
        CAST(rain_accuracy_30d AS DOUBLE) AS rain_accuracy_30d,
        CAST(temp_mae_30d AS DOUBLE)    AS temp_mae_30d,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY row_num DESC
        ) AS rn
    FROM source
    WHERE status IS NOT NULL
)

SELECT
    date,
    status,
    rain_accuracy_30d,
    temp_mae_30d
FROM deduped
WHERE rn = 1
