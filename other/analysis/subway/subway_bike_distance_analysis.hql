# This file is used for analyzing the substitute and complementary relationship between nearby subway station and bike station---for short, distance analysis
# calculate the subway data(entries) by year and month and gridid before 2020
create table subway_for_join_grid AS
select year, month, gridid, sum(netentries) as monthlycumentries, sum(netexits) as monthlycumexits 
from subway_data_net_clean_new_grid where year < 2020 group by year, month, gridid order by year, month, gridid;

# calculate the bike data by year and month and startgridid in 2013-2019
create table bike_for_join_startgrid AS 
select startyear as year, startmonth as month, gridid as startgridid, count(*) as startcount from vag273.bike_staging_with_end_grids where startyear < 2020 group by startyear, startmonth, gridid order by startyear, startmonth, gridid;

# calculate the bike data by year and month and endgridid in 2013-2019
create table bike_for_join_endgrid AS 
select startyear as year, startmonth as month, endgridid, count(*) as endcount from vag273.bike_staging_with_end_grids where startyear < 2020 group by startyear, startmonth, endgridid order by startyear, startmonth, endgridid;

# analyze whether bike is substitude to subway both from start point
create table subway_bike_join_by_startgrid_sub AS 
select subway_for_join_grid.year as year, subway_for_join_grid.month as month, bike_for_join_startgrid.startgridid as startgridid, subway_for_join_grid.monthlycumentries as subwaymonthlycumentries, 
bike_for_join_startgrid.startcount as bikestartcount from subway_for_join_grid, bike_for_join_startgrid where (subway_for_join_grid.year = bike_for_join_startgrid.year and 
subway_for_join_grid.month = bike_for_join_startgrid.month and subway_for_join_grid.gridid = bike_for_join_startgrid.startgridid);

# analyze whether bike is substitude to subway both from end point
create table subway_bike_join_by_endgrid_sub AS 
select subway_for_join_grid.year as year, subway_for_join_grid.month as month, bike_for_join_endgrid.endgridid as endgridid, subway_for_join_grid.monthlycumexits as subwaymonthlycumexits,  
bike_for_join_endgrid.endcount as bikeendcount from subway_for_join_grid, bike_for_join_endgrid where (subway_for_join_grid.year = bike_for_join_endgrid.year and 
subway_for_join_grid.month = bike_for_join_endgrid.month and subway_for_join_grid.gridid = bike_for_join_endgrid.endgridid);

# analyze whether bike is complementary to subway, bike endgrid, endcount, subway entries
create table subway_bike_join_by_endgrid_com AS 
select subway_for_join_grid.year as year, subway_for_join_grid.month as month, bike_for_join_endgrid.endgridid as endgridid, subway_for_join_grid.monthlycumentries as subwaymonthlycumentries, 
bike_for_join_endgrid.endcount as bikeendcount from subway_for_join_grid, bike_for_join_endgrid where (subway_for_join_grid.year = bike_for_join_endgrid.year and 
subway_for_join_grid.month = bike_for_join_endgrid.month and subway_for_join_grid.gridid = bike_for_join_endgrid.endgridid);

# analyze whether bike is complementary to subway, subway exits, bike startgrid, startcount
create table subway_bike_join_by_startgrid_com AS 
select subway_for_join_grid.year as year, subway_for_join_grid.month as month, bike_for_join_startgrid.startgridid as startgridid, subway_for_join_grid.monthlycumexits as subwaymonthlycumexits, 
bike_for_join_startgrid.startcount as bikestartcount from subway_for_join_grid, bike_for_join_startgrid where (subway_for_join_grid.year = bike_for_join_startgrid.year and 
subway_for_join_grid.month = bike_for_join_startgrid.month and subway_for_join_grid.gridid = bike_for_join_startgrid.startgridid);