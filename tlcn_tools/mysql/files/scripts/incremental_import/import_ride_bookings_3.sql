LOAD DATA INFILE '/var/lib/mysql-files/data_files/ride_bookings_part3.csv'
INTO TABLE ride_bookings
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @date,
    @time,
    @booking_id,
    @booking_status,
    @customer_id,
    @vehicle_type,
    @pickup_location,
    @drop_location,
    @avg_vtat,
    @avg_ctat,
    @cancelled_rides_by_customer,
    @reason_for_cancelling_by_customer,
    @cancelled_rides_by_driver,
    @driver_cancellation_reason,
    @incomplete_rides,
    @incomplete_rides_reason,
    @booking_value,
    @ride_distance,
    @driver_ratings,
    @customer_rating,
    @payment_method
)
SET
    date = STR_TO_DATE(@date, '%Y-%m-%d'),
    time = STR_TO_DATE(@time, '%H:%i:%s'),
    booking_id = @booking_id,
    booking_status = @booking_status,
    customer_id = @customer_id,
    vehicle_type = @vehicle_type,
    pickup_location = @pickup_location,
    drop_location = @drop_location,
    avg_vtat = NULLIF(@avg_vtat, ''),
    avg_ctat = NULLIF(@avg_ctat, ''),
    cancelled_rides_by_customer = NULLIF(@cancelled_rides_by_customer, ''),
    reason_for_cancelling_by_customer = NULLIF(@reason_for_cancelling_by_customer, ''),
    cancelled_rides_by_driver = NULLIF(@cancelled_rides_by_driver, ''),
    driver_cancellation_reason = NULLIF(@driver_cancellation_reason, ''),
    incomplete_rides = NULLIF(@incomplete_rides, ''),
    incomplete_rides_reason = NULLIF(@incomplete_rides_reason, ''),
    booking_value = NULLIF(@booking_value, ''),
    ride_distance = NULLIF(@ride_distance, ''),
    driver_ratings = NULLIF(@driver_ratings, ''),
    customer_rating = NULLIF(@customer_rating, ''),
    payment_method = NULLIF(TRIM(BOTH '\r\n' FROM @payment_method), '');