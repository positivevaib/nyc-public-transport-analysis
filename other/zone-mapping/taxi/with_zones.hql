CREATE TABLE rp3261.clean_data_2011_16_zone_wise
(year int comment 'from 2011 to 2016(first 6 months)', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', latitude double , longitude double, taxi_zone int);

insert into clean_data_2011_16_zone_wise
select year, month, day, hour,latitude, longitude,
CASE 
WHEN latitude>=40.7031731656 and latitude<=40.7113354232 and longitude>=-74.0090959591 and longitude<=-74.0024602764 THEN 87
WHEN latitude>=40.724272 and latitude<=40.732949 and longitude>=-74.002849 and longitude<=-73.991463 THEN 114
WHEN latitude>=40.7343701499 and latitude<=40.7460750268 and longitude>=-73.9978828447 and longitude<=-73.984042023 THEN 234
WHEN latitude>=40.772905 and latitude<=40.787938 and longitude>=-73.96741 and longitude<=-73.949289 THEN 236
WHEN latitude>=40.777528 and latitude<=40.789407 and longitude>=-73.98883 and longitude<=-73.969199 THEN 239
WHEN latitude>=40.782909 and latitude<=40.798096 and longitude>=-73.955741 and longitude<=-73.9356 THEN 75
WHEN latitude>=40.72149 and latitude<=40.734541 and longitude>=-73.992688 and longitude<=-73.977956 THEN 79
WHEN latitude>=40.749782 and latitude<=40.757252 and longitude>=-73.993468 and longitude<=-73.984087 THEN 100
WHEN latitude>=40.760304 and latitude<=40.767774 and longitude>=-73.984297 and longitude<=-73.971339 THEN 163
WHEN latitude>=40.801094 and latitude<=40.817901 and longitude>=-73.970537 and longitude<=-73.950625 THEN 166
ELSE 0 END
from clean_data_2011_16;

CREATE TABLE rp3261.clean_data_2011_16_zone_wise_cumulative
(year int comment 'from 2011 to 2016(first 6 months)', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', taxi_zone int, taxi_count int);
insert into clean_data_2011_16_zone_wise_cumulative
select year, month,day,hour, taxi_zone,count(*) from clean_data_2011_16_zone_wise group by year, month,day,hour, taxi_zone;

CREATE TABLE rp3261.clean_data_2016_19_zone_wise_cumulative
(year int comment 'from 2016(last 6 months) to 2019', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', taxi_zone int, taxi_count int);
insert into clean_data_2016_19_zone_wise_cumulative
select year, month,day,hour, LocationID,count(*) from clean_data_2016_19 group by year, month,day,hour, LocationID;

CREATE TABLE rp3261.clean_data_zone_wise_cumulative
(year int comment 'from 2011 to 2019', month int comment 'from 1 to 12', taxi_zone int, taxi_count int);

insert into clean_data_zone_wise_cumulative
select year,month,taxi_zone,count(*)
from clean_data_2011_16_zone_wise
where taxi_zone in (87, 114, 234, 236, 239, 75, 79, 100, 163, 166)
group by year,month,taxi_zone;

insert into clean_data_zone_wise_cumulative
select year,month,LocationID,count(*)
from clean_data_2016_19
where LocationID in (87, 114, 234, 236, 239, 75, 79, 100, 163, 166)
group by year,month,LocationID;