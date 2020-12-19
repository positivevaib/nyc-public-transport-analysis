-- 2016(last 6 months) to 2019

CREATE EXTERNAL TABLE IF NOT EXISTS rp3261.raw_data_2016_19
(vendor int,pickup_datetime string,dropoff_datetime string,passenger_count int,trip_distance int,rate string,store_and_fwd_flag string,PULocationID int,DOLocationID int,payment_type string,fare_amount_extra string,mta_tax_tip_amount string,tolls_amount int,improvement_surcharge int,	total_amount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/rp3261/RBDA_Project/raw_data/2016-19';

create table raw_data_2016_19_temp1 (pickup_datetime string,PULocationID int);
insert into raw_data_2016_19_temp1
select trim(pickup_datetime),PULocationID from raw_data_2016_19;

create table raw_data_2016_19_temp2 (pickup_datetime array<string>,PULocationID int);
insert into raw_data_2016_19_temp2
select split(pickup_datetime, ' '),PULocationID from raw_data_2016_19_temp1;

create table raw_data_2016_19_temp3(datee string, timee string, Location int);
insert into raw_data_2016_19_temp3
select pickup_datetime[0],pickup_datetime[1],PULocationID
from raw_data_2016_19_temp2;

create table raw_data_2016_19_temp4(datee string, timee string, Location int);
insert into raw_data_2016_19_temp4
select trim(datee), trim(timee), Location from raw_data_2016_19_temp3;

create table raw_data_2016_19_temp5(date1 array<string>,time1 array<string>, Location int);
insert into raw_data_2016_19_temp5
select split(datee,'-'),split(timee,':'),Location
from raw_data_2016_19_temp4;

create table raw_data_2016_19_final(year int, month int, day int, hour int, min int, sec int, loc int);
insert into raw_data_2016_19_final
select date1[0],date1[1],date1[2],time1[0],time1[1],time1[2],Location
from raw_data_2016_19_temp5;