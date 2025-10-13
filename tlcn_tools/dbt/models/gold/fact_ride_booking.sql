{{ config(materialized='table', schema='gold') }}

with ride as (
    select *
    from {{ ref('ride_bookings') }}
),

vehicle as (
    select
        vehicle_type,
        vehicle_key
    from {{ ref('vehicles') }}
),

pickup_location as (
    select
        location,
        location_key as pickup_key
    from {{ ref('locations') }}
),

drop_location as (
    select
        location,
        location_key as drop_key
    from {{ ref('locations') }}
),

payment as (
    select
        payment_method,
        payment_key
    from {{ ref('payments') }}
),

date_dim as (
    select
        full_date,
        date_key
    from {{ ref('date') }}
),

time_dim as (
    select
        time,
        time_key
    from {{ ref('time') }}
),

status_dim as (
    select
        status_name,
        status_key
    from {{ ref('status') }}
),

joined as (
    select
        -- surrogate key
        booking_key,

        -- join dimension keys
        d.date_key,
        t.time_key,
        p.pickup_key,
        dl.drop_key,
        v.vehicle_key,
        pay.payment_key,
        s.status_key,

        -- fact metrics
        cust_cancel,
        cust_cancel_reason,
        driver_cancel,
        driver_cancel_reason,
        avg_vtat,
        avg_ctat,
        booking_value,
        ride_distance,
        driver_rating,
        customer_rating

    from ride
    left join vehicle v on ride.vehicle_type = v.vehicle_type
    left join pickup_location p on ride.pickup_location = p.location
    left join drop_location dl on ride.drop_location = dl.location
    left join payment pay on ride.payment_method = pay.payment_method
    left join date_dim d on ride.date = d.full_date
    left join time_dim t on ride.time = t.time
    left join status_dim s on ride.booking_status = s.status_name
)

select * from joined