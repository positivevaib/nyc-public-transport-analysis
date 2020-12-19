create table rp3261.grid_wise_2012_15_temp
(year int comment 'from 2012 to 2015', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', hour1 int comment 'groups of 4 hours(0,4,8,12,16,20', latitude double , longitude double, gridRow int, gridCol int);

insert overwrite table grid_wise_2012_15_temp
select year, month, day, hour, cast((hour/4) as int) * 4,
latitude,longitude,
(latitude - 40.69715)/0.003, (longitude - (-74.022208))/0.003
from clean_data_2011_16
where year >=2012 and year<=2015;

create table rp3261.grid_wise_2012_15_temp2
(year int comment 'from 2012 to 2015', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'groups of 4 hours(0,4,8,12,16,20)',gridRow int, gridCol int, gridId string);

insert into rp3261.grid_wise_2012_15_temp2
select year, month, day, hour1, gridRow, gridCol, CONCAT_WS('_',cast(gridRow as string), cast(gridCol as string))
from grid_wise_2012_15_temp;

create table rp3261.grid_wise_2012_15
(year int comment 'from 2012 to 2015', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'groups of 4 hours(0,4,8,12,16,20)',gridId string);

insert into rp3261.grid_wise_2012_15
select year, month, day, hour, gridId
from rp3261.grid_wise_2012_15_temp2;

create table rp3261.grid_wise_2012_15_taxi_count
(year int comment 'from 2012 to 2015', month int comment 'from 1 to 12',gridId string, taxi_count int);
insert into rp3261.grid_wise_2012_15_taxi_count
select year, month, gridId, count(*)
from grid_wise_2012_15
group by year,month,gridId;