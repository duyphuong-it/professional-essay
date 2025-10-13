{{ config(materialized='view', schema='silver') }}

WITH source_date AS (
    SELECT
        DISTINCT date AS date
    FROM {{ source('bronze', 'ride_bookings') }}
),

transformed_date AS (
    SELECT
        date AS full_date,
        extract(year FROM date) AS year,
        extract(month FROM date) AS month,
        extract(day FROM date) AS day,
        extract(quarter FROM date) AS quarter,
        encode(digest(CAST(date AS VARCHAR), 'sha256'), 'hex') AS date_key
    FROM source_date
)

SELECT * FROM transformed_date