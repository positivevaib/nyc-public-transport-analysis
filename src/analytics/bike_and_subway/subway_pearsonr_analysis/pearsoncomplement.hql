-- Create table with yearly totals for bike counts and subway exits from 2014 to 2019
create table bike_subway_yearly_totals_2014_2019_exit (year int, bike_count bigint, subway_count_exit bigint);
insert overwrite table bike_subway_yearly_totals_2014_2019_exit select year, sum(bike_count), sum(subway_exits) from subway_bike_eastwestzone where (year >= 2014 and year <= 2019 and eastwestZone > 0) group by year;

-- Compute bike and subway averages across 5 years from 2014 to 2019
select avg(bike_count), avg(subway_count_exit) from bike_subway_yearly_totals_2014_2019_exit;

-- Create table to compute Pearson's r using the averages from last query
create table bike_subway_pearson_r_numerators_exit (value double);
insert overwrite table bike_subway_pearson_r_numerators_exit select (bike_count - 1245846.83)*(subway_count_exit - 31513072) from bike_subway_yearly_totals_2014_2019_exit;

create table bike_pearson_r_denominators_exit (value double);
insert overwrite table bike_pearson_r_denominators_exit select (bike_count - 1245846.83)*(bike_count - 1245846.83) from bike_subway_yearly_totals_2014_2019_exit;

create table subway_pearson_r_denominators_exit (value double);
insert overwrite table subway_pearson_r_denominators_exit select (subway_count_exit - 31513072)*(subway_count_exit - 31513072) from bike_subway_yearly_totals_2014_2019_exit;

select sum(value) from bike_subway_pearson_r_numerators_exit;
select sum(value) from bike_pearson_r_denominators_exit;
select sum(value) from subway_pearson_r_denominators_exit;
