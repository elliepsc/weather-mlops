-- Monthly data quality summary per city. Powers the quality heatmap in Power BI.

SELECT
    date_trunc('month', weather_date)                                   AS year_month,
    city,
    state,
    AVG(completeness_pct)                                               AS avg_completeness_pct,
    AVG(n_missing_fields)                                               AS avg_missing_fields,
    SUM(missing_rain_fields)                                            AS total_missing_rain_fields,
    SUM(missing_temperature_fields)                                     AS total_missing_temperature_fields,
    SUM(missing_wind_fields)                                            AS total_missing_wind_fields,
    SUM(missing_pressure_fields)                                        AS total_missing_pressure_fields,
    SUM(missing_cloud_solar_fields)                                     AS total_missing_cloud_solar_fields,
    SUM(CASE WHEN data_quality_status = 'Good'     THEN 1 ELSE 0 END)  AS good_quality_days,
    SUM(CASE WHEN data_quality_status = 'Warning'  THEN 1 ELSE 0 END)  AS warning_quality_days,
    SUM(CASE WHEN data_quality_status = 'Poor'     THEN 1 ELSE 0 END)  AS poor_quality_days,
    SUM(CASE WHEN data_quality_status = 'Critical' THEN 1 ELSE 0 END)  AS critical_quality_days
FROM {{ ref('mart_data_quality_daily') }}
GROUP BY date_trunc('month', weather_date), city, state
