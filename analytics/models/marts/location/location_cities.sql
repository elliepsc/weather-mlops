-- Location lookup for map visuals.
-- One row per city with a French-friendly place label and coordinates.

SELECT DISTINCT
    city                               AS lieu,
    state,
    latitude,
    longitude,
    city || ', ' || state              AS lieu_state
FROM {{ ref('dim_city') }}
WHERE city IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
