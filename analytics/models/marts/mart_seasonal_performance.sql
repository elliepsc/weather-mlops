-- Model performance broken down by Australian meteorological season.
-- The existing mart_model_performance_overview aggregates by calendar month
-- but NEVER by season — masking the fact that model accuracy likely degrades
-- significantly in Summer (Dec-Feb) due to storm/heatwave prevalence.
--
-- Grain: season × year (+ all-years aggregate per season).

WITH daily AS (
    SELECT
        p.prediction_date,
        p.city,
        p.rain_correct,
        p.temp_abs_error,
        p.has_actuals,
        d.au_season,
        d.au_season_code,
        d.year

    FROM {{ ref('int_actuals_vs_predictions') }} p
    LEFT JOIN {{ ref('dim_date') }} d ON d.date = p.prediction_date
    WHERE p.has_actuals
),

by_season_year AS (
    SELECT
        au_season,
        au_season_code,
        year,

        COUNT(*)                                                AS n_city_days,
        COUNT(DISTINCT prediction_date)                         AS n_days,
        COUNT(DISTINCT city)                                    AS n_cities,

        ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4)             AS rain_accuracy,
        ROUND(AVG(temp_abs_error), 3)                           AS temp_mae,
        ROUND(STDDEV(temp_abs_error), 3)                        AS temp_mae_stddev,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY temp_abs_error), 3)
                                                                AS temp_mae_p90,
        ROUND(MAX(temp_abs_error), 3)                           AS temp_max_error

    FROM daily
    GROUP BY au_season, au_season_code, year
),

-- All-years aggregate per season for a stable baseline
by_season_all AS (
    SELECT
        au_season,
        au_season_code,
        NULL::INTEGER                                           AS year,

        COUNT(*)                                                AS n_city_days,
        COUNT(DISTINCT prediction_date)                         AS n_days,
        COUNT(DISTINCT city)                                    AS n_cities,

        ROUND(AVG(CAST(rain_correct AS DOUBLE)), 4)             AS rain_accuracy,
        ROUND(AVG(temp_abs_error), 3)                           AS temp_mae,
        ROUND(STDDEV(temp_abs_error), 3)                        AS temp_mae_stddev,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY temp_abs_error), 3)
                                                                AS temp_mae_p90,
        ROUND(MAX(temp_abs_error), 3)                           AS temp_max_error

    FROM daily
    GROUP BY au_season, au_season_code
)

SELECT * FROM by_season_year
UNION ALL
SELECT * FROM by_season_all
ORDER BY au_season, year NULLS LAST
