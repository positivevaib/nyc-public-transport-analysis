-- causal inference for zone 236 after the introduction of bikes in 2013 
create table zone236_year1 (month int, taxi bigint, subway bigint);
insert overwrite table zone236_year1 select month, taxi, subway from causal_2013 where (zone = 236 and ((year = 2012 and month > 6) or (year = 2013 and month <= 6)));

create table zone236_year2 (month int, taxi bigint, subway bigint);
insert overwrite table zone236_year2 select month, taxi, subway from causal_2013 where (zone = 236 and ((year = 2013 and month > 6) or (year = 2014 and month <= 6)));

create table zone236_year3 (month int, taxi bigint, subway bigint);
insert overwrite table zone236_year3 select month, taxi, subway from causal_2013 where (zone = 236 and ((year = 2014 and month > 6) or (year = 2015 and month <= 6)));

create table zone236 (month int, taxi_year1 bigint, taxi_year2 bigint, taxi_year3 bigint, subway_year1 bigint, subway_year2 bigint, subway_year3 bigint);
insert overwrite table zone236 select a.month, a.taxi, b.taxi, c.taxi, a.subway, b.subway, c.subway from zone236_year1 a join zone236_year2 b on (a.month = b.month) join zone236_year3 c on (a.month = c.month);

create table zone236_changes (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table zone236_changes select month, ((taxi_year2 - taxi_year1)/taxi_year1) * 100, ((taxi_year3 - taxi_year2)/taxi_year2) * 100, ((subway_year2 - subway_year1)/subway_year1) * 100, ((subway_year3 - subway_year2)/subway_year2) * 100 from zone236;
