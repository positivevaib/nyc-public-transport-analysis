create external table bike_staging (placeholder string, startyear int, startmonth int, startday int, starthour int, stationid int, latitude double, longitude double, gridid int, usertype int, birthyear int, gender int) row format delimited fields terminated by ',' location '/user/vag273/rbda_proj/clean';

create table bike_main (year int, month int, day int, hour int, latitude double, longitude double, zone int);
insert overwrite table bike_main select startyear, startmonth, startday, starthour, latitude, longitude, gridid from bike_staging;

create table bike_counts (year int, month int, day int, hour int, zone int, count bigint);
insert overwrite table bike_counts select year, month, day, hour, zone, count(*) from bike_main group by year, month, day, hour, zone;

create table bike_for_join (year int, month int, zone int, count bigint);
insert overwrite table bike_for_join select year, month, zone, count(*) from bike_main where zone != 0 group by year, month, zone;

-- grids

create external table bike_staging_grids (placeholder string, startyear int, startmonth int, startday int, starthour int, stationid int, latitude double, longitude double, gridid string, usertype int, birthyear int, gender int) row format delimited fields terminated by ',' location '/user/vag273/rbda_proj/bike_grids';

create table bike_main_grids (year int, month int, day int, hour int, latitude double, longitude double, zone string);
insert overwrite table bike_main_grids select startyear, startmonth, startday, starthour, latitude, longitude, gridid from bike_staging_grids;

create table bike_counts_grids (year int, month int, day int, hour int, zone string, count bigint);
insert overwrite table bike_counts_grids select year, month, day, hour, zone, count(*) from bike_main_grids group by year, month, day, hour, zone;

create table bike_for_join_grids (year int, month int, zone string, count bigint);
insert overwrite table bike_for_join_grids select year, month, zone, count(*) from bike_main_grids group by year, month, zone;
