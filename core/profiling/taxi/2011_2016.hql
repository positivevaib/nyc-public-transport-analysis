CREATE TABLE rp3261.clean_data_2011_16
(year int comment 'from 2011 to 2016(first 6 months)', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', latitude double , longitude double);
insert into clean_data_2011_16
select cast(substr(pickup_datetime,1,4) as int), cast(substr(pickup_datetime,6,2) as int), cast(substr(pickup_datetime,9,2) as int), 
cast(substr(pickup_datetime,12,2) as int), pickup_latitude, pickup_longitude
from raw_data_2015 where pickup_latitude>=40.69715 and pickup_latitude<=40.862752 and pickup_longitude>=-74.022208 and pickup_longitude<=-73.924361;
