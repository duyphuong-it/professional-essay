{{ config(materialized='view', schema='silver') }}

WITH transformed_vehicles AS (
    SELECT
        vehicle_type,
        category,
        capacity,
        segment,
        encode(DIGEST(CAST(vehicle_type AS VARCHAR), 'sha256'), 'hex') AS vehicle_key
    FROM {{ source('bronze', 'vehicles') }}
)

SELECT * FROM transformed_vehicles