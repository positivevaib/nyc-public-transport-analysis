-- 2011 to 2016 (first half)

CREATE TABLE IF NOT EXISTS rp3261.raw_data_2015
(vendor_id int, pickup_datetime string, dropoff_datetime string, passenger_count int, trip_distance int, pickup_longitude double, pickup_latitude double, rate_code string, store_and_fwd_flag string, dropoff_longitude double, dropoff_latitude double, payment_type string, fare_amount string, surcharge string, mta_tax string, tip_amount string, tolls_amount int, total_amount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/rp3261/RBDA_Project/raw_data/2015';