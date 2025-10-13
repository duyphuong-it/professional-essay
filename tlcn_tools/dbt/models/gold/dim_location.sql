{{ config(materialized='table', schema='gold') }}

WITH silver_locations AS (
    SELECT
        location_key,
        location,
        city,
        zone
    FROM {{ ref('locations') }}
)

SELECT * FROM silver_locations