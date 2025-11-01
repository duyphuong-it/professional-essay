{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='time',
    schema='silver'
  )
}}

WITH new_times AS (
    SELECT DISTINCT CAST(b.time AS time) AS time
    FROM {{ source('bronze', 'ride_bookings') }} b
    {% if is_incremental() %}
    LEFT JOIN {{ this }} s
      ON CAST(b.time AS time) = s.time
    WHERE s.time IS NULL
    {% endif %}
),

transformed_time AS (
    SELECT
        time,
        EXTRACT(HOUR FROM time) AS hour,
        EXTRACT(MINUTE FROM time) AS minute,
        EXTRACT(SECOND FROM time) AS second,
        CASE
            WHEN EXTRACT(HOUR FROM time) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM time) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM time) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        ENCODE(DIGEST(CAST(time AS VARCHAR), 'sha256'), 'hex') AS time_key
    FROM new_times
)

SELECT * FROM transformed_time
