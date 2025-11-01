{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='full_date',
    schema='silver'
  ) 
}}

WITH new_dates AS (
    SELECT DISTINCT b.date AS full_date
    FROM {{ source('bronze', 'ride_bookings') }} b
    {% if is_incremental() %}
    LEFT JOIN {{ this }} s
      ON b.date = s.full_date
    WHERE s.full_date IS NULL
    {% endif %}
)

SELECT
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    ENCODE(DIGEST(CAST(full_date AS VARCHAR), 'sha256'), 'hex') AS date_key
FROM new_dates
