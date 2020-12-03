-- Manhattan level data
select startyear, startmonth, starthour, count(*) from bike_main group by startyear, startmonth, starthour;

-- Neighborhood level data
select startyear, startmonth, starthour, gridid, count(*) from bike_main where (gridid = 87 or gridid = 114 or gridid = 234 or gridid = 236 or gridid = 239) group by startyear, startmonth, starthour, gridid;
