CREATE TABLE rp3261.Ten_zones_count_of_all_three
(year int comment 'from 2011 to 2019', month int comment 'from 1 to 12', taxi_zone int comment '10 zones', bike_count int, taxi_count int, subway_count int);

insert overwrite table Ten_zones_count_of_all_three
select
t.year,
t.month,
t.taxi_zone,
b.count as bike_count,
t.taxi_count as taxi_count,
s.monthlycumentries as subway_count

from rp3261.clean_data_zone_wise_cumulative t
left join vag273.bike_for_join b
on (b.year=t.year and b.month=t.month and b.zone=t.taxi_zone)
left join jl11257.subway_for_join s
on (t.year=s.year and t.month=s.month and t.taxi_zone=s.zone);