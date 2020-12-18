-- Create table with yearly totals for bike counts and subway entries from 2014 to 2019
create table bike_subway_yearly_totals_2014_2019 (year int, bike_count bigint, subway_count bigint);
insert overwrite table bike_subway_yearly_totals_2014_2019 select year, sum(bike_count), sum(subway_entries) from subway_bike_eastwestzone where (year >= 2014 and year <= 2019 and eastwestZone > 0) group by year;

-- Compute bike and subway averages across 5 years from 2014 to 2019
select avg(bike_count), avg(subway_count) from bike_subway_yearly_totals_2014_2019;

-- Create table to compute Pearson's r using the averages from last query
create table bike_subway_pearson_r_numerators (value double);
insert overwrite table bike_subway_pearson_r_numerators select (bike_count - 1245846.83)*(subway_count - 40756615.17) from bike_subway_yearly_totals_2014_2019;

create table bike_pearson_r_denominators (value double);
insert overwrite table bike_pearson_r_denominators select (bike_count - 1245846.83)*(bike_count - 1245846.83) from bike_subway_yearly_totals_2014_2019;

create table subway_pearson_r_denominators (value double);
insert overwrite table subway_pearson_r_denominators select (subway_count - 40756615.17)*(subway_count - 40756615.17) from bike_subway_yearly_totals_2014_2019;

select sum(value) from bike_subway_pearson_r_numerators;
select sum(value) from bike_pearson_r_denominators;
select sum(value) from subway_pearson_r_denominators;

