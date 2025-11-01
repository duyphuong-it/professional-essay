{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='booking_key',
    on_schema_change='sync_all_columns',
    schema='gold'
  ) 
}}

WITH ride AS (
    SELECT *
    FROM {{ ref('ride_bookings') }}
    {% if is_incremental() %}
      WHERE event_timestamp > (SELECT COALESCE(MAX(event_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
),

vehicle AS (
    SELECT vehicle_type, vehicle_key
    FROM {{ ref('vehicles') }}
),

pickup_location AS (
    SELECT location, location_key AS pickup_key
    FROM {{ ref('locations') }}
),

drop_location AS (
    SELECT location, location_key AS drop_key
    FROM {{ ref('locations') }}
),

payment AS (
    SELECT payment_method, payment_key
    FROM {{ ref('payments') }}
),

date_dim AS (
    SELECT full_date, date_key
    FROM {{ ref('date') }}
),

time_dim AS (
    SELECT time, time_key
    FROM {{ ref('time') }}
),

status_dim AS (
    SELECT status_name, status_key
    FROM {{ ref('status') }}
),

joined AS (
    SELECT
        r.booking_key,
        d.date_key,
        t.time_key,
        p.pickup_key,
        dl.drop_key,
        v.vehicle_key,
        pay.payment_key,
        s.status_key,
        r.cust_cancel,
        r.cust_cancel_reason,
        r.driver_cancel,
        r.driver_cancel_reason,
        r.avg_vtat,
        r.avg_ctat,
        r.booking_value,
        r.ride_distance,
        r.driver_rating,
        r.customer_rating,
        r.event_timestamp
    FROM ride r
    LEFT JOIN vehicle v ON r.vehicle_type = v.vehicle_type
    LEFT JOIN pickup_location p ON r.pickup_location = p.location
    LEFT JOIN drop_location dl ON r.drop_location = dl.location
    LEFT JOIN payment pay ON r.payment_method = pay.payment_method
    LEFT JOIN date_dim d ON r.date = d.full_date
    LEFT JOIN time_dim t ON r.time = t.time
    LEFT JOIN status_dim s ON r.booking_status = s.status_name
)

SELECT * FROM joined
