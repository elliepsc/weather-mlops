-- Calendar dimension built from the actual date range observed in weather_raw.
-- Avoids hardcoding a spine; self-extends as new data arrives.
-- Provides season labels for BOTH hemispheres so the model works when
-- French or Spanish cities (Northern hemisphere) are added.
-- Downstream models should join on date AND filter on hemisphere from
-- dim_geography to pick the correct season column.

WITH date_spine AS (
    SELECT DISTINCT date
    FROM {{ ref('stg_weather_raw') }}
),

enriched AS (
    SELECT
        date,
        EXTRACT(YEAR  FROM date)    AS year,
        EXTRACT(MONTH FROM date)    AS month,
        EXTRACT(DAY   FROM date)    AS day,
        EXTRACT(DOW   FROM date)    AS day_of_week,   -- 0=Sun … 6=Sat
        EXTRACT(WEEK  FROM date)    AS iso_week,
        EXTRACT(QUARTER FROM date)  AS quarter,
        date_trunc('month', date)   AS month_start,
        date_trunc('year',  date)   AS year_start,
        date_trunc('week',  date)   AS week_start,

        -- Weekend flag
        EXTRACT(DOW FROM date) IN (0, 6)            AS is_weekend,

        -- ── SOUTHERN HEMISPHERE seasons (Australia) ────────────────────
        -- Dec-Feb = Summer, Mar-May = Autumn, Jun-Aug = Winter, Sep-Nov = Spring
        CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 'Summer' WHEN  1 THEN 'Summer' WHEN  2 THEN 'Summer'
            WHEN  3 THEN 'Autumn' WHEN  4 THEN 'Autumn' WHEN  5 THEN 'Autumn'
            WHEN  6 THEN 'Winter' WHEN  7 THEN 'Winter' WHEN  8 THEN 'Winter'
            WHEN  9 THEN 'Spring' WHEN 10 THEN 'Spring' WHEN 11 THEN 'Spring'
        END                                         AS season_southern,

        CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 'DJF' WHEN  1 THEN 'DJF' WHEN  2 THEN 'DJF'
            WHEN  3 THEN 'MAM' WHEN  4 THEN 'MAM' WHEN  5 THEN 'MAM'
            WHEN  6 THEN 'JJA' WHEN  7 THEN 'JJA' WHEN  8 THEN 'JJA'
            WHEN  9 THEN 'SON' WHEN 10 THEN 'SON' WHEN 11 THEN 'SON'
        END                                         AS season_code_southern,

        -- ── NORTHERN HEMISPHERE seasons (France, Spain, …) ─────────────
        -- Dec-Feb = Winter, Mar-May = Spring, Jun-Aug = Summer, Sep-Nov = Autumn
        CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 'Winter' WHEN  1 THEN 'Winter' WHEN  2 THEN 'Winter'
            WHEN  3 THEN 'Spring' WHEN  4 THEN 'Spring' WHEN  5 THEN 'Spring'
            WHEN  6 THEN 'Summer' WHEN  7 THEN 'Summer' WHEN  8 THEN 'Summer'
            WHEN  9 THEN 'Autumn' WHEN 10 THEN 'Autumn' WHEN 11 THEN 'Autumn'
        END                                         AS season_northern,

        CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 'DJF' WHEN  1 THEN 'DJF' WHEN  2 THEN 'DJF'
            WHEN  3 THEN 'MAM' WHEN  4 THEN 'MAM' WHEN  5 THEN 'MAM'
            WHEN  6 THEN 'JJA' WHEN  7 THEN 'JJA' WHEN  8 THEN 'JJA'
            WHEN  9 THEN 'SON' WHEN 10 THEN 'SON' WHEN 11 THEN 'SON'
        END                                         AS season_code_northern
        -- NOTE: season_code is identical across hemispheres (DJF/MAM/JJA/SON)
        -- but the LABEL flips. Use season_southern / season_northern in display.
        -- When joining: LEFT JOIN dim_geography g USING (city) then use
        -- CASE g.hemisphere WHEN 'Southern' THEN d.season_southern ELSE d.season_northern END

    FROM date_spine
)

SELECT *
FROM enriched
ORDER BY date
