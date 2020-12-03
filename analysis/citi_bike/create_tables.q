create external table bike_staging (placeholder string, startyear int, startmonth int, startday int, starthour int, stationid int, latitude double, longitude double, gridid int, usertype int, birthyear int, gender int) row format delimited fields terminated by ',' location '/user/vag273/rbda_proj/bike_clean';

create table bike_main (startyear int, startmonth int, starthour int, gridid int);
insert overwrite table bike_main select startyear, startmonth, starthour, gridid from bike_staging where (startyear >= 2015 and startyear <= 2019);

-- if need to create partitioned tables
-- SET hive.exec.dynamic.partition=true;
-- SET hive.exec.dynamic.partition.mode=nonstrict;
-- create table bike_partitioned (gridid string, count bigint) partitioned by (startyear int, startmonth int);
-- insert into table bike_partitioned partition(startyear, startmonth) select gridid, count, startyear, startmonth from bike_main;

-- if need to create separate partitioned tables for years and months
-- create table bike_years (startmonth int, gridid string, count bigint) partitioned by (startyear int);
-- insert into table bike_years partition(startyear) select startmonth, gridid, count, startyear from bike_main;
-- create table bike_months (startyear int, gridid string, count bigint) partitioned by (startmonth int);
-- insert into table bike_months partition(startmonth) select startyear, gridid, count, startmonth from bike_main;

-- if need to create separate tables instead of using partitions
-- create table bike_2015 (startmonth int, gridid string, count bigint);
-- insert overwrite table bike_2015 select startmonth, gridid, count(*) from bike where bike.startyear = 2015 group by bike.startmonth, bike.gridid;
