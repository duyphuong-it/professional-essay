{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_name',
    schema='silver'
  ) 
}}

WITH cleaned_status AS (
    SELECT DISTINCT booking_status AS status_name
    FROM {{ source('bronze', 'ride_bookings') }} s
    {% if is_incremental() %}
        WHERE booking_status NOT IN (SELECT status_name FROM {{ this }})
    {% endif %}
),

transformed_status AS (
    SELECT
        status_name,
        encode(digest(CAST(status_name AS VARCHAR), 'sha256'), 'hex') AS status_key
    FROM cleaned_status
)

SELECT * FROM transformed_status