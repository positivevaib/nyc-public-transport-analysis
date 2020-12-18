CREATE TABLE subway_data_net_clean_new_eastwestzone
(year int comment 'from 2010 to 2020', month int comment 'from 1 to 12', netEntries int, netExits int, latitude double , longitude double, eastwestZone int);

insert into subway_data_net_clean_new_eastwestzone
select year, month, netEntries, netExits, latitude, longitude,
CASE 
WHEN latitude>=40.709716 and latitude<=40.724192 and longitude>=-73.99556 and longitude<=-73.972686 THEN 1
WHEN latitude>=40.716287 and latitude<=40.726929 and longitude>=-74.018617 and longitude<=-74.00416 THEN 2
WHEN latitude>=40.72693 and latitude<=40.733034 and longitude>=-74.014514 and longitude<=-74.002003 THEN 3
ELSE 0 END
from subway_data_net_clean_new_grid;

CREATE TABLE subway_data_net_clean_new_eastwestzone_cumulative
(year int comment 'from 2010 to 2019', month int comment 'from 1 to 12', eastwestZone int, monthlycumentries int, monthlycumexits int);

insert into subway_data_net_clean_new_eastwestzone_cumulative
select year, month, eastwestZone, sum(netEntries) as monthlycumentries, sum(netExits) as monthlycumexits 
from subway_data_net_clean_new_eastwestzone where year < 2020 group by year, month, eastwestZone order by year, month, eastwestZone;

# calculate the bike data by year and month and gridid in 2013-2019
create table bike_east_west_staging_cum AS 
select startyear as year, startmonth as month, gridid as eastwestZone, count(*) as count 
from vag273.bike_east_west_staging 
where startyear < 2020 group by startyear, startmonth, gridid order by startyear, startmonth, gridid;

# join subway and bike data by year, month, eastwestZone
create table subway_bike_eastwestzone 
(year int comment 'from 2010 to 2019', month int comment 'from 1 to 12', eastwestZone int, subway_entries int, subway_exits int, bike_count int);

insert overwrite table subway_bike_eastwestzone 
select s.year, s.month, s.eastwestZone, s.monthlycumentries as subway_entries, s.monthlycumexits as subway_exits, b.count as bike_count 
from subway_data_net_clean_new_eastwestzone_cumulative s 
left join bike_east_west_staging_cum b 
on (s.year = b.year and s.month = b.month and s.eastwestZone = b.eastwestZone);