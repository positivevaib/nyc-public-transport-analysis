-- causal_2013 table for 30 grids
create table if not exists causal_2013 (year int, month int, grid string, taxi bigint);

insert overwrite table causal_2013
select t_year, t_month, taxi_grid, taxi_count
from bike_taxi_grids 
where ((t_year=2012 and t_month>6) or t_year=2013 or t_year=2014 or (t_year=2015 and t_month<=6))
and taxi_grid in 
("7_5","7_4","14_12","16_9","17_6","2_2","18_16","19_18","2_3","20_18","20_21","21_6","21_7","22_8","9_8",
"8_3","27_19","4_3","13_16","10_3","17_5","30_22","23_8","24_9","33_18","34_29","9_9","41_22","5_2","24_24");

-- make 3 tables from causal_2013 - for year1, year2, year3 
create table if not exists causal_2013_year1 (month int, grid string, taxi bigint);
create table if not exists causal_2013_year2 (month int, grid string, taxi bigint);
create table if not exists causal_2013_year3 (month int, grid string, taxi bigint);

insert overwrite table causal_2013_year1
select month, grid, taxi
from causal_2013
where ((year = 2012 and month > 6) or (year = 2013 and month <= 6));

insert overwrite table causal_2013_year2
select month, grid, taxi
from causal_2013
where ((year = 2013 and month > 6) or (year = 2014 and month <= 6));

insert overwrite table causal_2013_year3
select month, grid, taxi
from causal_2013
where ((year = 2014 and month > 6) or (year = 2015 and month <= 6));

-- make bike_changes table (change over 1 year and over 2 year) 
create table if not exists bike_changes (month int, grid string, taxi1 double, taxi2 double);

insert overwrite table bike_changes
select y1.month, y1.grid, ((y2.taxi-y1.taxi)/y1.taxi)*100, ((y3.taxi-y1.taxi)/y1.taxi)*100
from causal_2013_year1 y1
join causal_2013_year2 y2
on y1.month=y2.month and y1.grid=y2.grid
join causal_2013_year3 y3
on y1.month=y3.month and y1.grid=y3.grid;

-- make treatment and control tables from bike_changes table 
create table if not exists treatment_2013 (month int, grid string, taxi1 double, taxi2 double);
create table if not exists control_2013 (month int, grid string, taxi1 double, taxi2 double);

insert overwrite table treatment_2013
select * from bike_changes
where grid in ("7_5","7_4","14_12","16_9","17_6","2_2","18_16","19_18","2_3","20_18","20_21","21_6","21_7","22_8","9_8");

insert overwrite table control_2013
select * from bike_changes
where grid in ("8_3","27_19","4_3","13_16","10_3","17_5","30_22","23_8","24_9","33_18","34_29","9_9","41_22","5_2","24_24");

-- make average treatment and control tables 
create table if not exists avg_treatment_2013 (month int, taxi1 double, taxi2 double);
create table if not exists avg_control_2013 (month int, taxi1 double, taxi2 double);

insert overwrite table avg_treatment_2013
select month, avg(taxi1), avg(taxi2)
from treatment_2013
group by month;

insert overwrite table avg_control_2013
select month, avg(taxi1), avg(taxi2)
from control_2013
group by month;

-- final cusal effects tables (treatment - control)
create table if not exists causal_effects_2013 (month int, taxi1 double, taxi2 double);

insert overwrite table causal_effects_2013
select t.month, t.taxi1-c.taxi1, t.taxi2-c.taxi2
from avg_treatment_2013 t
join avg_control_2013 c
on t.month=c.month;

select * from causal_effects_2013;

select *
from bike_taxi_grids
where t_year is 2015 and b_year is null taxi_count>2000;

select *
from bike_taxi_grids
where t_year is 2015 and b_year is not  null taxi_count>2000;
