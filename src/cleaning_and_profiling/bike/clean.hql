-- Create a staging table
create external table bike_staging (placeholder string, startyear int, startmonth int, startday int, starthour int, stationid int, startlatitude double, startlongitude double, startgridid string, starthourbin int, subwayzone int) row format delimited fields terminated by ',' location '/user/vag273/rbda_proj/bike_clean';

-- Create tables with trip counts grouped by date, time and start locations
create table bike_counts_twenty_four_hours (startyear int, startmonth int, starthour int, startgridid string, count bigint);
insert overwrite table bike_counts_twenty_four_hours select startyear, startmonth, starthour, startgridid, count(*) from bike_staging group by startyear, startmonth, starthour, startgridid;

create table bike_counts_four_hour_bins (startyear int, startmonth int, starthourbin int, startgridid string, count bigint);
insert overwrite table bike_counts_four_hour_bins select startyear, startmonth, starthourbin, startgridid, count(*) from bike_staging group by startyear, startmonth, starthourbin, startgridid;

-- Create table with trip counts grouped by date, time and subway zone
create table bike_counts_subway_zones (startyear int, startmonth int, starthourbin int, subwayzone int, count bigint);
insert overwrite table bike_counts_subway_zones select startyear, startmonth, starthourbin, subwayzone, count(*) from bike_staging group by startyear, startmonth, starthourbin, subwayzone;

-- Create table for taxi analysis
create table bike_for_join_grids (year int, month int, zone string, count bigint);
insert overwrite table bike_for_join_grids select startyear, startmonth, startgridid, count(*) from bike_staging group by startyear, startmonth, startgridid;

-- Create table for subway analysis
create table bike_east_west_staging (startyear int, startmonth int, gridid int);
insert overwrite table bike_east_west_staging select startyear, startmonth, subwayzone from bike_staging;

