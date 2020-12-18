-- Create table with yearly totals for bike and taxi rides from 2014 to 2019
create table bike_taxi_yearly_totals_2014_2019 (year int, bike_count bigint, taxi_count bigint);
insert overwrite table bike_taxi_yearly_totals_2014_2019 select b_year, sum(bike_count), sum(taxi_count) from rp3261.bike_taxi_grids where (b_year >= 2014 and b_year <= 2019) group by b_year;

-- Compute bike and taxi averages across 5 years from 2014 to 2019
select avg(bike_count), avg(taxi_count) from bike_taxi_yearly_totals_2014_2019;

-- Create table to compute Pearson's r using the averages from last query
create table bike_taxi_pearson_r_numerators (value double);
insert overwrite table bike_taxi_pearson_r_numerators select (bike_count - 3973450.33)*(taxi_count - 34625428.66) from bike_taxi_yearly_totals_2014_2019;

create table bike_pearson_r_denominators (value double);
insert overwrite table bike_pearson_r_denominators select (bike_count - 3973450.33)*(bike_count - 3973450.33) from bike_taxi_yearly_totals_2014_2019;

create table taxi_pearson_r_denominators (value double);
insert overwrite table taxi_pearson_r_denominators select (taxi_count - 34625428.66)*(taxi_count - 34625428.66) from bike_taxi_yearly_totals_2014_2019;

select sum(value) from bike_taxi_pearson_r_numerators;
select sum(value) from bike_pearson_r_denominators;
select sum(value) from taxi_pearson_r_denominators;
