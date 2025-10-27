LOAD DATA INFILE '/var/lib/mysql-files/data_files/location_zones.csv'
INTO TABLE locations
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    location,
    city,
    zone
);

LOAD DATA INFILE '/var/lib/mysql-files/data_files/payment_methods.csv'
INTO TABLE payments
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    payment_method,
    category
);

LOAD DATA INFILE '/var/lib/mysql-files/data_files/vehicle_types.csv'
INTO TABLE vehicles
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    vehicle_type,
    category,
    capacity,
    segment
);