-- Count distinct bike stations over the lifetime of the Citi Bike System and count the existing stations in September 2020
select count (distinct stationid) from bike_staging;
select count (distinct stationid) from bike_staging where (year = 2020 and month = 9);

