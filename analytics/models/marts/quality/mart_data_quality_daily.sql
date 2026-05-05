-- Data quality assessment per (date, city): counts missing fields by category,
-- computes completeness % and a status label (Good / Warning / Poor / Critical).

WITH quality AS (
    SELECT
        weather_date,
        city,
        state,
        latitude,
        longitude,

        40 AS n_total_fields,

        (
            CASE WHEN min_temp               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN max_temp               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rainfall               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_sum               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN precipitation_hours    IS NULL THEN 1 ELSE 0 END +
            CASE WHEN evaporation            IS NULL THEN 1 ELSE 0 END +
            CASE WHEN sunshine_hours         IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_gust_dir          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_gust_speed        IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_dir_9am           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_dir_3pm           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_9am         IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_3pm         IS NULL THEN 1 ELSE 0 END +
            CASE WHEN humidity_9am           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN humidity_3pm           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN dew_point_9am          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN dew_point_3pm          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN pressure_9am           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN pressure_3pm           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN surface_pressure_9am   IS NULL THEN 1 ELSE 0 END +
            CASE WHEN surface_pressure_3pm   IS NULL THEN 1 ELSE 0 END +
            CASE WHEN cloud_9am              IS NULL THEN 1 ELSE 0 END +
            CASE WHEN cloud_3pm              IS NULL THEN 1 ELSE 0 END +
            CASE WHEN temp_9am               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN temp_3pm               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_today             IS NULL THEN 1 ELSE 0 END +
            CASE WHEN weather_code           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN shortwave_radiation_sum IS NULL THEN 1 ELSE 0 END +
            CASE WHEN vpd_9am                IS NULL THEN 1 ELSE 0 END +
            CASE WHEN vpd_3pm                IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_100m_9am    IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_100m_3pm    IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_tomorrow          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_tomorrow_proba    IS NULL THEN 1 ELSE 0 END +
            CASE WHEN max_temp_tomorrow      IS NULL THEN 1 ELSE 0 END +
            CASE WHEN weather_type_tomorrow  IS NULL THEN 1 ELSE 0 END +
            CASE WHEN comfort_score          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN heatwave_risk          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN frost_risk             IS NULL THEN 1 ELSE 0 END +
            CASE WHEN storm_probability      IS NULL THEN 1 ELSE 0 END
        )                                               AS n_missing_fields,

        (
            CASE WHEN rainfall            IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_sum            IS NULL THEN 1 ELSE 0 END +
            CASE WHEN precipitation_hours IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_today          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_tomorrow       IS NULL THEN 1 ELSE 0 END +
            CASE WHEN rain_tomorrow_proba IS NULL THEN 1 ELSE 0 END
        )                                               AS missing_rain_fields,

        (
            CASE WHEN min_temp           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN max_temp           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN temp_9am           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN temp_3pm           IS NULL THEN 1 ELSE 0 END +
            CASE WHEN max_temp_tomorrow  IS NULL THEN 1 ELSE 0 END
        )                                               AS missing_temperature_fields,

        (
            CASE WHEN wind_gust_speed      IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_9am       IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_3pm       IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_100m_9am  IS NULL THEN 1 ELSE 0 END +
            CASE WHEN wind_speed_100m_3pm  IS NULL THEN 1 ELSE 0 END
        )                                               AS missing_wind_fields,

        (
            CASE WHEN pressure_9am         IS NULL THEN 1 ELSE 0 END +
            CASE WHEN pressure_3pm         IS NULL THEN 1 ELSE 0 END +
            CASE WHEN surface_pressure_9am IS NULL THEN 1 ELSE 0 END +
            CASE WHEN surface_pressure_3pm IS NULL THEN 1 ELSE 0 END
        )                                               AS missing_pressure_fields,

        (
            CASE WHEN cloud_9am               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN cloud_3pm               IS NULL THEN 1 ELSE 0 END +
            CASE WHEN sunshine_hours          IS NULL THEN 1 ELSE 0 END +
            CASE WHEN shortwave_radiation_sum IS NULL THEN 1 ELSE 0 END
        )                                               AS missing_cloud_solar_fields

    FROM {{ ref('mart_weather_daily_clean') }}
)

SELECT
    *,
    ROUND((1 - n_missing_fields * 1.0 / NULLIF(n_total_fields, 0)) * 100, 2) AS completeness_pct,
    CASE
        WHEN (1 - n_missing_fields * 1.0 / NULLIF(n_total_fields, 0)) >= 0.90 THEN 'Good'
        WHEN (1 - n_missing_fields * 1.0 / NULLIF(n_total_fields, 0)) >= 0.75 THEN 'Warning'
        WHEN (1 - n_missing_fields * 1.0 / NULLIF(n_total_fields, 0)) >= 0.50 THEN 'Poor'
        ELSE 'Critical'
    END                                                 AS data_quality_status
FROM quality
