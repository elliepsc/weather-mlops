-- WMO weather code lookup table (ISO 4677 / Open-Meteo convention).
-- weather_code in stg_weather_raw is an integer; this dimension provides
-- human-readable labels and severity tiers used in extreme-event analysis.
-- Covers the subset of codes actually produced by Open-Meteo daily aggregation.

SELECT
    weather_code,
    description,
    description                     AS weather_label,
    category,
    category                        AS weather_group,
    -- severity: 0=clear, 1=cloud/fog, 2=precip, 3=heavy/storm
    severity_tier
FROM (
    VALUES
        (0,   'Clear sky',                          'Clear',        0),
        (1,   'Mainly clear',                       'Clear',        0),
        (2,   'Partly cloudy',                      'Cloudy',       1),
        (3,   'Overcast',                           'Cloudy',       1),
        (45,  'Fog',                                'Fog',          1),
        (48,  'Depositing rime fog',                'Fog',          1),
        (51,  'Drizzle: light',                     'Drizzle',      2),
        (53,  'Drizzle: moderate',                  'Drizzle',      2),
        (55,  'Drizzle: dense',                     'Drizzle',      2),
        (61,  'Rain: slight',                       'Rain',         2),
        (63,  'Rain: moderate',                     'Rain',         2),
        (65,  'Rain: heavy',                        'Rain',         3),
        (71,  'Snow fall: slight',                  'Snow',         2),
        (73,  'Snow fall: moderate',                'Snow',         2),
        (75,  'Snow fall: heavy',                   'Snow',         3),
        (77,  'Snow grains',                        'Snow',         2),
        (80,  'Rain showers: slight',               'Showers',      2),
        (81,  'Rain showers: moderate',             'Showers',      2),
        (82,  'Rain showers: violent',              'Showers',      3),
        (85,  'Snow showers: slight',               'Snow',         2),
        (86,  'Snow showers: heavy',                'Snow',         3),
        (95,  'Thunderstorm: slight or moderate',   'Thunderstorm', 3),
        (96,  'Thunderstorm with slight hail',      'Thunderstorm', 3),
        (99,  'Thunderstorm with heavy hail',       'Thunderstorm', 3)
) AS t(weather_code, description, category, severity_tier)
