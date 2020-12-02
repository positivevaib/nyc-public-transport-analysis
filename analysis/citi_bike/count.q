 select startyear, count(*) from bike_staging where (startyear >= 2015 and startyear <= 2019) group by startyear;
 select startmonth, count(*) from bike_staging where (startyear >= 2015 and startyear <= 2019) group by startmonth;
 select startyear, startmonth, count(*) from bike_staging where (startyear >= 2015 and startyear <= 2019) group by startyear, startmonth;
