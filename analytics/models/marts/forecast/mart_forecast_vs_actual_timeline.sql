-- Rolling 90-day time-series of predictions vs actuals per city.
-- Primary table for Power BI line charts — visualises drift before
-- aggregated metrics catch it.

SELECT
    p.prediction_date,
    p.city,
    dc.state,

    -- Predictions
    p.pred_rain_tomorrow,
    p.pred_rain_proba,
    p.pred_max_temp_tomorrow,
    p.pred_weather_type_tomorrow,
    p.pred_heatwave_risk,
    p.pred_frost_risk,
    p.pred_storm_probability,
    p.comfort_score,

    -- Actuals
    p.actual_date,
    p.actual_rain,
    p.actual_max_temp,
    p.actual_min_temp,
    p.actual_rainfall,

    -- Performance
    p.rain_correct,
    p.temp_abs_error,
    p.has_actuals,

    -- Rolling context
    d.rain_accuracy_30d,
    d.temp_mae_30d,
    d.rain_accuracy_7d,
    d.temp_mae_7d

FROM {{ ref('int_actuals_vs_predictions') }} p
LEFT JOIN {{ ref('int_model_performance_daily') }} d
    ON  d.prediction_date = p.prediction_date
    AND d.city = p.city
LEFT JOIN {{ ref('dim_cities') }} dc
    ON dc.city = p.city
WHERE p.prediction_date >= {{ dbt.dateadd('day', -1 * var('forecast_timeline_days'), 'current_date') }}
ORDER BY p.prediction_date DESC, p.city
