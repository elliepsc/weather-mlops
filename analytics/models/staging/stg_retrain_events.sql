SELECT
    CAST(retrain_date AS DATE)      AS retrain_date,
    written_at,
    source

FROM {{ source('raw', 'src_retrain_events') }}
