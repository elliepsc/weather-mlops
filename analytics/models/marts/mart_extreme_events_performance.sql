-- Model accuracy specifically on extreme weather days.
-- The model predicts heatwave_risk, frost_risk, storm_probability.
-- Nowhere in the existing models is accuracy evaluated on the days
-- these events actually occur — only global/monthly averages are tracked.
--
-- Critical question: "Does the model perform worse on the days that matter most?"
-- A model with 85% global accuracy but 50% accuracy on heatwave days is
-- operationally dangerous.
--
-- Grain: extreme event type × city × season.

WITH actuals AS (
    SELECT
        date,
        city,
        is_heatwave_day,
        is_frost_day,
        is_storm_day,
        is_heavy_rain_day,
        is_compound_extreme,
        event_label
    FROM {{ ref('int_extreme_events_actual') }}
),

perf AS (
    SELECT
        p.prediction_date,
        p.city,
        p.rain_correct,
        p.temp_abs_error,
        p.pred_rain_proba,
        p.actual_rain,
        p.pred_heatwave_risk,
        p.pred_frost_risk,
        p.pred_storm_probability,
        p.has_actuals,
        d.au_season

    FROM {{ ref('int_actuals_vs_predictions') }} p
    LEFT JOIN {{ ref('dim_date') }} d ON d.date = p.prediction_date
    WHERE p.has_actuals
),

-- Join predictions to the actual extreme event flags on the ACTUAL day
joined AS (
    SELECT
        perf.*,
        -- Extreme flags apply to the actual_date (= prediction_date + 1)
        a.is_heatwave_day,
        a.is_frost_day,
        a.is_storm_day,
        a.is_heavy_rain_day,
        a.is_compound_extreme,
        a.event_label
    FROM perf
    LEFT JOIN actuals a
        ON  a.city = perf.city
        AND a.date = {{ dbt.dateadd('day', 1, 'perf.prediction_date') }}
),

-- Unpivot by event type for easier slicing in Power BI
by_heatwave AS (
    SELECT 'Heatwave' AS event_type, city, au_season,
           COUNT(*)                             AS n_event_days,
           ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4) AS rain_accuracy,
           ROUND(AVG(temp_abs_error), 3)        AS temp_mae,
           -- heatwave_risk threshold: flag as "predicted" if score >= 0.5
           ROUND(AVG(CASE WHEN pred_heatwave_risk >= 0.5 THEN 1.0 ELSE 0.0 END), 4)
                                               AS risk_flag_rate,
           ROUND(AVG(pred_heatwave_risk), 4)   AS mean_predicted_risk
    FROM joined WHERE is_heatwave_day
    GROUP BY city, au_season
),

by_frost AS (
    SELECT 'Frost' AS event_type, city, au_season,
           COUNT(*) AS n_event_days,
           ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4) AS rain_accuracy,
           ROUND(AVG(temp_abs_error), 3) AS temp_mae,
           ROUND(AVG(CASE WHEN pred_frost_risk >= 0.5 THEN 1.0 ELSE 0.0 END), 4) AS risk_flag_rate,
           ROUND(AVG(pred_frost_risk), 4) AS mean_predicted_risk
    FROM joined WHERE is_frost_day
    GROUP BY city, au_season
),

by_storm AS (
    SELECT 'Storm' AS event_type, city, au_season,
           COUNT(*) AS n_event_days,
           ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4) AS rain_accuracy,
           ROUND(AVG(temp_abs_error), 3) AS temp_mae,
           ROUND(AVG(CASE WHEN pred_storm_probability >= 0.5 THEN 1.0 ELSE 0.0 END), 4) AS risk_flag_rate,
           ROUND(AVG(pred_storm_probability), 4) AS mean_predicted_risk
    FROM joined WHERE is_storm_day
    GROUP BY city, au_season
),

by_heavy_rain AS (
    SELECT 'Heavy rain' AS event_type, city, au_season,
           COUNT(*) AS n_event_days,
           ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4) AS rain_accuracy,
           ROUND(AVG(temp_abs_error), 3) AS temp_mae,
           ROUND(AVG(pred_rain_proba), 4) AS risk_flag_rate,
           ROUND(AVG(pred_rain_proba), 4) AS mean_predicted_risk
    FROM joined WHERE is_heavy_rain_day
    GROUP BY city, au_season
),

-- Global baseline for comparison
baseline AS (
    SELECT
        'Baseline (all days)' AS event_type,
        city,
        au_season,
        COUNT(*)                                AS n_event_days,
        ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4) AS rain_accuracy,
        ROUND(AVG(temp_abs_error), 3)           AS temp_mae,
        NULL::DOUBLE                            AS risk_flag_rate,
        NULL::DOUBLE                            AS mean_predicted_risk
    FROM joined
    GROUP BY city, au_season
)

SELECT * FROM by_heatwave
UNION ALL SELECT * FROM by_frost
UNION ALL SELECT * FROM by_storm
UNION ALL SELECT * FROM by_heavy_rain
UNION ALL SELECT * FROM baseline
ORDER BY event_type, city, au_season
