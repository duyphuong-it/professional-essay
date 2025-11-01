{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    incremental_strategy = 'merge',
    schema = 'silver'
  )
}}

SELECT
    date,
    CAST(time AS time) AS time,
    booking_id,
    booking_status,
    customer_id,
    vehicle_type,
    pickup_location,
    drop_location,
    avg_vtat,
    avg_ctat,
    cancelled_rides_by_customer AS cust_cancel,
    reason_for_cancelling_by_customer AS cust_cancel_reason,
    cancelled_rides_by_driver AS driver_cancel,
    driver_cancellation_reason AS driver_cancel_reason,
    incomplete_rides AS incomplete_ride,
    incomplete_rides_reason AS incomplete_ride_reason,
    booking_value,
    ride_distance,
    driver_ratings AS driver_rating,
    customer_rating,
    payment_method,
    event_timestamp,
    encode(digest(booking_id, 'sha256'), 'hex') AS booking_key
FROM {{ source('bronze', 'ride_bookings') }}

{% if is_incremental() %}
WHERE event_timestamp > (SELECT COALESCE(MAX(event_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}