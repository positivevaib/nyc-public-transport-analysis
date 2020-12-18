# create tables for inserting relative data and locating subway station
create external table subway_manhattan (stationID string, complexID string, gtfsStopID string, line string, remoteUnit string, 
division string, stopName string, daytimeRoutes string, borough string, structure string, latitude string, longitude string, 
northDirection string, southDirection string, ada string, adaNotes string)
row format delimited fields terminated by ','
location '/user/jl11257/subway_manhattan/';

create external table remoteUnit_station (remoteUnit string, station string)
row format delimited fields terminated by ','
location '/user/jl11257/subway_remoteunit_station/';

# get subway_position table(remoteunit, station, latitude, longitude)
create table subway_position as
select subway_manhattan.remoteUnit, remoteUnit_station.station, subway_manhattan.latitude, subway_manhattan.longitude 
from subway_manhattan, remoteUnit_station
where subway_manhattan.remoteUnit = remoteUnit_station.remoteUnit;

# build subway turnstile data table 
create external table subway_turnstile_new (controlArea string, remoteUnit string, subunitChannelPosition string, year int, month int, day int, hour int, entries int, exits int)
row format delimited fields terminated by ','
location '/user/jl11257/subway_clean_data/';

# below two tables are used for calculating net entries and exits
create table subway_turnstile_ex_new as 
select remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour, entries, LAG(entries)
over (partition by remoteUnit, controlArea, subunitChannelPosition 
order by remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour) AS preEntries, 
exits, LAG(exits) over (partition by remoteUnit, controlArea, subunitChannelPosition 
order by remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour) AS preExits
from subway_turnstile_new;

create table subway_turnstile_net_new as 
select remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour, (entries - preEntries) as netEntries, (exits - preExits) as netExits 
from subway_turnstile_ex_new;

# get subway_data_net_new table(join subway net entries, net exits and position info(latitude, longitude) together) 
create table subway_data_net_new  
row format delimited fields terminated by ','
as select subway_turnstile_net_new.controlArea, subway_turnstile_net_new.remoteUnit, subway_turnstile_net_new.subunitChannelPosition, subway_position.station, 
subway_turnstile_net_new.year, subway_turnstile_net_new.month, subway_turnstile_net_new.day, subway_turnstile_net_new.hour, subway_turnstile_net_new.netEntries, subway_turnstile_net_new.netExits,
subway_position.latitude, subway_position.longitude  
from subway_turnstile_net_new, subway_position 
where subway_turnstile_net_new.remoteUnit = subway_position.remoteUnit;

# use mapreduce to add taxi zone(gridId) column and clean the data from table subway_data_net_new
# create table subway_data_net_clean_new to insert clean data
create table subway_data_net_clean_new (controlArea string, remoteUnit string, subunitChannelPosition string, station string, year int, month int, day int, hour int, netEntries int, netExits int, latitude double, longitude double, gridId int)
row format delimited fields terminated by ','
location '/user/jl11257/subway_data_net_clean_new/';

# use mapreduce to add gridId column and clean the data from table subway_data_net_new
# create table subway_data_net_clean_new_grid to insert clean data
create table subway_data_net_clean_new_grid (controlArea string, remoteUnit string, subunitChannelPosition string, station string, year int, month int, day int, hour int, netEntries int, netExits int, latitude double, longitude double, zone int, gridId string)
row format delimited fields terminated by ','
location '/user/jl11257/subway_data_net_clean_new_grid/';

# add comments to certain columns of table subway_data_net_clean_new_grid
alter table subway_data_net_clean_new_grid change netEntries netEntries int comment 'between 0 and 14400';
alter table subway_data_net_clean_new_grid change netExits netExits int comment 'between 0 and 14400';