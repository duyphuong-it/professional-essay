CREATE DATABASE IF NOT EXISTS uber_ride_bookings;

USE uber_ride_bookings;

CREATE TABLE IF NOT EXISTS locations (
    location VARCHAR(30) PRIMARY KEY,
    city VARCHAR(30) NOT NULL,
    zone VARCHAR(10) NOT NULL
);
TRUNCATE TABLE locations;

CREATE TABLE IF NOT EXISTS payments (
    payment_method VARCHAR(15) PRIMARY KEY,
    category VARCHAR(10) NOT NULL
);
TRUNCATE TABLE payments;

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_type VARCHAR(15) PRIMARY KEY,
    category VARCHAR(10) NOT NULL,
    capacity INT NOT NULL,
    segment VARCHAR(15) NOT NULL
);
TRUNCATE TABLE vehicles;

CREATE TABLE IF NOT EXISTS ride_bookings (
    date DATE NOT NULL,
    time TIME NOT NULL,
    booking_id VARCHAR(20) NOT NULL,
    booking_status VARCHAR(25) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    pickup_location VARCHAR(30) NOT NULL,
    drop_location VARCHAR(30) NOT NULL,
    avg_vtat DECIMAL(3,1),
    avg_ctat DECIMAL(3,1),
    cancelled_rides_by_customer BOOLEAN,
    reason_for_cancelling_by_customer VARCHAR(50),
    cancelled_rides_by_driver BOOLEAN,
    driver_cancellation_reason VARCHAR(50),
    incomplete_rides BOOLEAN,
    incomplete_rides_reason VARCHAR(50),
    booking_value DECIMAL(5,1),
    ride_distance DECIMAL(4,2),
    driver_ratings DECIMAL(2,1),
    customer_rating DECIMAL(2,1),
    payment_method VARCHAR(15),
    PRIMARY KEY (booking_id)
);
TRUNCATE TABLE ride_bookings;