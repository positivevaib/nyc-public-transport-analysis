create table rp3261.bike_taxi_grids
(t_year int, b_year int, t_month int, b_month int, taxi_grid string, taxi_count int,bike_grid string, bike_count int);


insert overwrite table rp3261.bike_taxi_grids

select t.year, b.year, t.month, b.month, t.gridId, t.taxi_count, b.zone, b.count
from rp3261.grid_wise_2012_15_taxi_count t
full outer join
vag273.bike_for_join_grids b
on t.year = b.year and t.month = b.month and t.gridId = b.zone;