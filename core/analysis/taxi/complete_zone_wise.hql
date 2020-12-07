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