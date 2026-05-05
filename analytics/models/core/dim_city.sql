-- Simplified city dimension for Power BI joins.
-- Stable 4-column view of dim_cities (no date fields).

SELECT
    city,
    state,
    latitude,
    longitude
FROM {{ ref('dim_cities') }}
