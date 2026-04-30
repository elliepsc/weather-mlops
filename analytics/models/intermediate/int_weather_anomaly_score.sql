-- Z-score anomaly detection per city × feature × date, relative to the
-- historical seasonal baseline computed in int_weather_features_stats.
--
-- PURPOSE — drift explainability:
-- The existing pipeline detects model drift (KS-test on feature distributions)
-- but cannot answer: "Did the model drift because it degraded, or because the
-- weather itself was statistically unusual this month?"
-- A drift event during a record heatwave in Adelaide is expected and does NOT
-- necessarily imply the model needs retraining — the data was out of sample.
-- This model provides the evidence to distinguish both cases.
--
-- METHODOLOGY:
--   z = (observed_value − historical_monthly_mean) / historical_monthly_stddev
--   |z| > 2 → unusual day for that feature in that city × month context.
--
-- IMPORTANT CAVEAT: stddev from int_weather_features_stats includes ALL years.
-- For a more robust baseline, exclude the most recent 12 months when computing
-- the historical reference. This is left as a future improvement; flag it in
-- the review before using z-scores for automated alerting.
--
-- Grain: city × date × (selected key features).
-- Columns are deliberately limited to features most relevant to the model:
-- max_temp, min_temp, rainfall, wind_gust_speed, humidity_3pm, pressure_3pm.

WITH raw AS (
    SELECT
        date,
        city,
        max_temp,
        min_temp,
        rainfall,
        wind_gust_speed,
        humidity_3pm,
        pressure_3pm
    FROM {{ ref('stg_weather_raw') }}
),

-- Historical monthly baselines from the existing intermediate
baselines AS (
    SELECT
        city,
        EXTRACT(MONTH FROM month)   AS month_num,
        avg_max_temp,
        stddev_max_temp,
        avg_min_temp,
        stddev_min_temp,
        avg_rainfall,
        -- rainfall stddev not pre-computed in int_weather_features_stats;
        -- use avg as proxy for now (Poisson-like: stddev ≈ sqrt(mean))
        SQRT(NULLIF(avg_rainfall, 0))       AS stddev_rainfall_proxy,
        avg_wind_gust_speed,
        -- wind stddev not pre-computed; use 30% CV as proxy
        avg_wind_gust_speed * 0.3           AS stddev_wind_proxy,
        avg_humidity_3pm,
        -- humidity proxy stddev
        avg_humidity_3pm * 0.15             AS stddev_humidity_proxy,
        avg_pressure_3pm,
        avg_pressure_3pm * 0.005            AS stddev_pressure_proxy
    FROM {{ ref('int_weather_features_stats') }}
),

joined AS (
    SELECT
        r.date,
        r.city,
        EXTRACT(MONTH FROM r.date)  AS month_num,

        -- Raw values
        r.max_temp,
        r.min_temp,
        r.rainfall,
        r.wind_gust_speed,
        r.humidity_3pm,
        r.pressure_3pm,

        -- Z-scores (NULL if stddev = 0 or missing)
        (r.max_temp     - b.avg_max_temp)       / NULLIF(b.stddev_max_temp, 0)          AS z_max_temp,
        (r.min_temp     - b.avg_min_temp)       / NULLIF(b.stddev_min_temp, 0)          AS z_min_temp,
        (r.rainfall     - b.avg_rainfall)       / NULLIF(b.stddev_rainfall_proxy, 0)    AS z_rainfall,
        (r.wind_gust_speed - b.avg_wind_gust_speed) / NULLIF(b.stddev_wind_proxy, 0)   AS z_wind_gust,
        (r.humidity_3pm - b.avg_humidity_3pm)   / NULLIF(b.stddev_humidity_proxy, 0)   AS z_humidity_3pm,
        (r.pressure_3pm - b.avg_pressure_3pm)   / NULLIF(b.stddev_pressure_proxy, 0)   AS z_pressure_3pm

    FROM raw r
    LEFT JOIN baselines b
        ON  b.city     = r.city
        AND b.month_num = EXTRACT(MONTH FROM r.date)
),

scored AS (
    SELECT
        date,
        city,

        -- Individual z-scores (rounded for storage)
        ROUND(z_max_temp,    2)     AS z_max_temp,
        ROUND(z_min_temp,    2)     AS z_min_temp,
        ROUND(z_rainfall,    2)     AS z_rainfall,
        ROUND(z_wind_gust,   2)     AS z_wind_gust,
        ROUND(z_humidity_3pm,2)     AS z_humidity_3pm,
        ROUND(z_pressure_3pm,2)     AS z_pressure_3pm,

        -- Anomaly flags per feature (|z| > 2)
        ABS(z_max_temp)     > 2     AS anomaly_max_temp,
        ABS(z_min_temp)     > 2     AS anomaly_min_temp,
        ABS(z_rainfall)     > 2     AS anomaly_rainfall,
        ABS(z_wind_gust)    > 2     AS anomaly_wind_gust,
        ABS(z_humidity_3pm) > 2     AS anomaly_humidity_3pm,
        ABS(z_pressure_3pm) > 2     AS anomaly_pressure_3pm,

        -- Count of anomalous features on this city×day
        (
            (ABS(z_max_temp)     > 2)::INTEGER
          + (ABS(z_min_temp)     > 2)::INTEGER
          + (ABS(z_rainfall)     > 2)::INTEGER
          + (ABS(z_wind_gust)    > 2)::INTEGER
          + (ABS(z_humidity_3pm) > 2)::INTEGER
          + (ABS(z_pressure_3pm) > 2)::INTEGER
        )                           AS n_anomalous_features,

        -- Composite anomaly score: max absolute z across features
        GREATEST(
            ABS(COALESCE(z_max_temp,    0)),
            ABS(COALESCE(z_min_temp,    0)),
            ABS(COALESCE(z_rainfall,    0)),
            ABS(COALESCE(z_wind_gust,   0)),
            ABS(COALESCE(z_humidity_3pm,0)),
            ABS(COALESCE(z_pressure_3pm,0))
        )                           AS composite_anomaly_score,

        -- Day-level flag
        (
            (ABS(z_max_temp)     > 2)::INTEGER
          + (ABS(z_min_temp)     > 2)::INTEGER
          + (ABS(z_rainfall)     > 2)::INTEGER
          + (ABS(z_wind_gust)    > 2)::INTEGER
          + (ABS(z_humidity_3pm) > 2)::INTEGER
          + (ABS(z_pressure_3pm) > 2)::INTEGER
        ) >= 2                      AS is_anomalous_day

    FROM joined
)

SELECT *
FROM scored
