-- taxi changes 2012/13
create table taxi_2012 (zone int, count bigint);
insert overwrite table taxi_2012 select zone, sum(taxi) as count from all_three where (year = 2012) group by zone;

create table taxi_2013 (zone int, count bigint);
insert overwrite table taxi_2013 select zone, sum(taxi) as count from all_three where (year = 2013) group by zone;

create table taxi_2012_1_5 (zone int, count bigint);
insert overwrite table taxi_2012_1_5 select zone, sum(taxi) as count from all_three where (year = 2012 and month < 6) group by zone;

create table taxi_2012_6_12 (zone int, count bigint);
insert overwrite table taxi_2012_6_12 select zone, sum(taxi) as count from all_three where (year = 2012 and month >= 6) group by zone;

create table taxi_2013_1_5 (zone int, count bigint);
insert overwrite table taxi_2013_1_5 select zone, sum(taxi) as count from all_three where (year = 2013 and month < 6) group by zone;

create table taxi_2013_6_12 (zone int, count bigint);
insert overwrite table taxi_2013_6_12 select zone, sum(taxi) as count from all_three where (year = 2013 and month >= 6) group by zone;

create table bike_intros_2013 (zone int, count_12 bigint, count_13 bigint, count_12_pre bigint, count_13_pre bigint, count_12_post bigint, count_13_post bigint);
insert overwrite table bike_intros_2013 select a.zone, a.count, b.count, c.count, d.count, e.count, f.count from taxi_2012 a join taxi_2013 b on (a.zone=b.zone) join taxi_2012_1_5 c on (a.zone=c.zone) join taxi_2013_1_5 d on (a.zone=d.zone) join taxi_2012_6_12 e on (a.zone=e.zone) join taxi_2013_6_12 f on (a.zone=f.zone);

create table taxi_changes_2013 (zone int, yearly double, pre double, post double);
insert overwrite table taxi_changes_2013 select zone, ((count_13 - count_12)/count_12) * 100, ((count_13_pre - count_12_pre)/count_12_pre) * 100, ((count_13_post - count_12_post)/count_12_post) * 100 from bike_intros_2013;

-- taxi changes 2014/15
create table taxi_2014 (zone int, count bigint);
insert overwrite table taxi_2014 select zone, sum(taxi) as count from all_three where (year = 2014) group by zone;

create table taxi_2015 (zone int, count bigint);
insert overwrite table taxi_2015 select zone, sum(taxi) as count from all_three where (year = 2015) group by zone;

create table taxi_2014_1_8 (zone int, count bigint);
insert overwrite table taxi_2014_1_8 select zone, sum(taxi) as count from all_three where (year = 2014 and month < 9) group by zone;

create table taxi_2014_9_12 (zone int, count bigint);
insert overwrite table taxi_2014_9_12 select zone, sum(taxi) as count from all_three where (year = 2014 and month >= 9) group by zone;

create table taxi_2015_1_8 (zone int, count bigint);
insert overwrite table taxi_2015_1_8 select zone, sum(taxi) as count from all_three where (year = 2015 and month < 9) group by zone;

create table taxi_2015_9_12 (zone int, count bigint);
insert overwrite table taxi_2015_9_12 select zone, sum(taxi) as count from all_three where (year = 2015 and month >= 9) group by zone;

create table bike_intros_2015 (zone int, count_14 bigint, count_15 bigint, count_14_pre bigint, count_15_pre bigint, count_14_post bigint, count_15_post bigint);
insert overwrite table bike_intros_2015 select a.zone, a.count, b.count, c.count, d.count, e.count, f.count from taxi_2014 a join taxi_2015 b on (a.zone=b.zone) join taxi_2014_1_8 c on (a.zone=c.zone) join taxi_2015_1_8 d on (a.zone=d.zone) join taxi_2014_9_12 e on (a.zone=e.zone) join taxi_2015_9_12 f on (a.zone=f.zone);

create table taxi_changes_2015 (zone int, yearly double, pre double, post double);
insert overwrite table taxi_changes_2015 select zone, ((count_15 - count_14)/count_14) * 100, ((count_15_pre - count_14_pre)/count_14_pre) * 100, ((count_15_post - count_14_post)/count_14_post) * 100 from bike_intros_2015;

