{{ config(materialized='view', schema='silver') }}

WITH source_time AS (
    SELECT
        DISTINCT time AS time
    FROM {{ source('bronze', 'ride_bookings') }}
),

transformed_time AS (
    SELECT
        CAST(time AS time) AS time, -- time is varchar then
        extract(hour FROM CAST(time AS time)) AS hour,
        extract(minute FROM CAST(time AS time)) AS minute,
        extract(second FROM CAST(time AS time)) AS second,
        CASE
            WHEN extract(hour FROM CAST(time AS time)) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN extract(hour FROM CAST(time AS time)) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN extract(hour FROM CAST(time AS time)) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        encode(DIGEST(CAST(time AS VARCHAR), 'sha256'), 'hex') AS time_key
    FROM source_time
)

SELECT * FROM transformed_time
