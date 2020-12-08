# get subway_position table(remoteunit, station, latitude, longitude)
CREATE TABLE subway_position AS
select subway_manhattan.remoteUnit, remoteUnit_station.station, subway_manhattan.latitude, subway_manhattan.longitude 
from subway_manhattan, remoteUnit_station
where subway_manhattan.remoteUnit = remoteUnit_station.remoteUnit;

# build subway turnstile data table 
create external table subway_turnstile_new (controlArea string, remoteUnit string, subunitChannelPosition string, year int, month int, day int, hour int, entries int, exits int)
row format delimited fields terminated by ','
location '/user/jl11257/subway_clean_data/';

# below two tables are used for calculating net entries and exits
CREATE TABLE subway_turnstile_ex_new AS 
select remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour, entries, LAG(entries)
over (partition by remoteUnit, controlArea, subunitChannelPosition 
order by remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour) AS preEntries, 
exits, LAG(exits) over (partition by remoteUnit, controlArea, subunitChannelPosition 
order by remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour) AS preExits
from subway_turnstile_new;

CREATE TABLE subway_turnstile_net_new AS 
select remoteUnit, controlArea, subunitChannelPosition, year, month, day, hour, (entries - preEntries) as netEntries, (exits - preExits) as netExits 
from subway_turnstile_ex_new;

# get subway_data_net_new table(join subway net entries, net exits and position info(latitude, longitude) together) 
CREATE TABLE subway_data_net_new  
row format delimited fields terminated by ','
AS select subway_turnstile_net_new.controlArea, subway_turnstile_net_new.remoteUnit, subway_turnstile_net_new.subunitChannelPosition, subway_position.station, 
subway_turnstile_net_new.year, subway_turnstile_net_new.month, subway_turnstile_net_new.day, subway_turnstile_net_new.hour, subway_turnstile_net_new.netEntries, subway_turnstile_net_new.netExits,
subway_position.latitude, subway_position.longitude  
from subway_turnstile_net_new, subway_position 
where subway_turnstile_net_new.remoteUnit = subway_position.remoteUnit;

# use mapreduce to add gridId column and clean the data from table subway_data_net_new
# create table subway_data_net_clean_new to insert clean data
CREATE TABLE subway_data_net_clean_new (controlArea string, remoteUnit string, subunitChannelPosition string, station string, year int, month int, day int, hour int, netEntries int, netExits int, latitude double, longitude double, gridId int)
row format delimited fields terminated by ','
location '/user/jl11257/subway_data_net_clean_new/';

# add comments to certain columns of table subway_data_net_clean_new
ALTER TABLE subway_data_net_clean_new CHANGE year year int COMMENT 'from 2012 to 2019';
ALTER TABLE subway_data_net_clean_new CHANGE month month int COMMENT 'from 1 to 12';
ALTER TABLE subway_data_net_clean_new CHANGE day day int COMMENT 'from 1 to 31';
ALTER TABLE subway_data_net_clean_new CHANGE hour hour int COMMENT 'from 7 to 23, and 0, 1';
ALTER TABLE subway_data_net_clean_new CHANGE netEntries netEntries int COMMENT 'between 0 and 14400';
ALTER TABLE subway_data_net_clean_new CHANGE netExits netExits int COMMENT 'between 0 and 14400';