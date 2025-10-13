{{ config(materialized='view', schema='silver') }}

WITH cleaned_status AS (
    SELECT
        DISTINCT booking_status AS status_name
    FROM {{ source('bronze', 'ride_bookings') }}
),

transformed_status AS (
    SELECT
        status_name,
        encode(DIGEST(cast(status_name AS VARCHAR), 'sha256'), 'hex') AS status_key
    FROM cleaned_status
)

SELECT * FROM transformed_status