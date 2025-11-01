{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_name',
    schema='silver'
  ) 
}}

WITH new_status AS (
    SELECT DISTINCT booking_status AS status_name
    FROM {{ source('bronze', 'ride_bookings') }} b
    {% if is_incremental() %}
    LEFT JOIN {{ this }} s
        ON b.booking_status = s.status_name
    WHERE s.status_name IS NULL
    {% endif %}
)

SELECT
    status_name,
    encode(digest(CAST(status_name AS VARCHAR), 'sha256'), 'hex') AS status_key
FROM new_status
