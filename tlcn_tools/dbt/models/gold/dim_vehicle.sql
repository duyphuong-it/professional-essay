{{ config(materialized='table', schema='gold') }}

WITH silver_vehicles AS (
    SELECT
        vehicle_key,
        vehicle_type,
        category,
        capacity,
        segment
    FROM {{ ref('vehicles') }}
)

SELECT * FROM silver_vehicles