 select startyear, count(*) from bike_staging group by startyear;
 select startmonth, count(*) from bike_staging group by startmonth;
 select startyear, startmonth, count(*) from bike_staging group by startyear, startmonth;
