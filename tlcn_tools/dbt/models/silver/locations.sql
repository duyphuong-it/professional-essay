{{ config(materialized='view', schema='silver') }}

WITH transformed_locations AS (
    SELECT
        location,
        city,
        zone,
        encode(DIGEST(CAST(location AS VARCHAR), 'sha256'), 'hex') AS location_key
    FROM {{ source('bronze', 'locations') }}
)

SELECT * FROM transformed_locations