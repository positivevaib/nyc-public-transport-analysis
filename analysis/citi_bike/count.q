 select startyear, count(*) from bike_partitioned group by startyear;
 select startmonth, count(*) from bike_partitioned group by startmonth;
 select startyear, startmonth, count(*) from bike_partitioned group by startyear, startmonth;
