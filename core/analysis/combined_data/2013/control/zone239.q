-- causal inference for zone 239 after the introduction of bikes in 2013 
create table zone239_year1 (month int, taxi bigint, subway bigint);
insert overwrite table zone239_year1 select month, taxi, subway from causal_2013 where (zone = 239 and ((year = 2012 and month > 6) or (year = 2013 and month <= 6)));

create table zone239_year2 (month int, taxi bigint, subway bigint);
insert overwrite table zone239_year2 select month, taxi, subway from causal_2013 where (zone = 239 and ((year = 2013 and month > 6) or (year = 2014 and month <= 6)));

create table zone239_year3 (month int, taxi bigint, subway bigint);
insert overwrite table zone239_year3 select month, taxi, subway from causal_2013 where (zone = 239 and ((year = 2014 and month > 6) or (year = 2015 and month <= 6)));

create table zone239 (month int, taxi_year1 bigint, taxi_year2 bigint, taxi_year3 bigint, subway_year1 bigint, subway_year2 bigint, subway_year3 bigint);
insert overwrite table zone239 select a.month, a.taxi, b.taxi, c.taxi, a.subway, b.subway, c.subway from zone239_year1 a join zone239_year2 b on (a.month = b.month) join zone239_year3 c on (a.month = c.month);

create table zone239_changes (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table zone239_changes select month, ((taxi_year2 - taxi_year1)/taxi_year1) * 100, ((taxi_year3 - taxi_year1)/taxi_year1) * 100, ((subway_year2 - subway_year1)/subway_year1) * 100, ((subway_year3 - subway_year1)/subway_year1) * 100 from zone239;

drop table zone239_year1;
drop table zone239_year2;
drop table zone239_year3;
drop table zone239;
