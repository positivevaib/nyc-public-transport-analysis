create table treatment_2013 (month int, zone87_taxi1 double, zone87_taxi2 double, zone87_subway1 double, zone87_subway2 double, zone234_taxi1 double, zone234_taxi2 double, zone234_subway1 double, zone234_subway2 double, zone114_taxi1 double, zone114_taxi2 double, zone114_subway1 double, zone114_subway2 double, zone163_taxi1 double, zone163_taxi2 double, zone163_subway1 double, zone163_subway2 double);
insert overwrite table treatment_2013 select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2, c.taxi1, c.taxi2, c.subway1, c.subway2, d.taxi1, d.taxi2, d.subway1, d.subway2 from zone87_changes a join zone234_changes b on (a.month = b.month) join zone114_changes c on (a.month = c.month) join zone163_changes d on (a.month = d.month);

create table avg_treatment_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table avg_treatment_2013 select month, (zone87_taxi1 + zone234_taxi1 + zone114_taxi1 + zone163_taxi1)/4, (zone87_taxi2 + zone234_taxi2 + zone114_taxi2 + zone163_taxi2)/4, (zone87_subway1 + zone234_subway1 + zone114_subway1 + zone163_subway1)/4, (zone87_subway2 + zone234_subway2 + zone114_subway2 + zone163_subway2)/4 from treatment_2013;

create table control_2013 (month int, zone236_taxi1 double, zone236_taxi2 double, zone236_subway1 double, zone236_subway2 double, zone239_taxi1 double, zone239_taxi2 double, zone239_subway1 double, zone239_subway2 double, zone166_taxi1 double, zone166_taxi2 double, zone166_subway1 double, zone166_subway2 double, zone75_taxi1 double, zone75_taxi2 double, zone75_subway1 double, zone75_subway2 double);
insert overwrite table control_2013 select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2, c.taxi1, c.taxi2, c.subway1, c.subway2, d.taxi1, d.taxi2, d.subway1, d.subway2 from zone236_changes a join zone239_changes b on (a.month = b.month) join zone166_changes c on (a.month = c.month) join zone75_changes d on (a.month = d.month);

create table avg_control_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table avg_control_2013 select month, (zone236_taxi1 + zone239_taxi1 + zone166_taxi1 + zone75_taxi1)/4, (zone236_taxi2 + zone239_taxi2 + zone166_taxi2 + zone75_taxi2)/4, (zone236_subway1 + zone239_subway1 + zone166_subway1 + zone75_subway1)/4, (zone236_subway2 + zone239_subway2 + zone166_subway2 + zone75_subway2)/4 from control_2013;

create table causal_2013_staging (month int, t_taxi1 double, t_taxi2 double, t_subway1 double, t_subway2 double, c_taxi1 double, c_taxi2 double, c_subway1 double, c_subway2 double);
insert overwrite table causal_2013_staging select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2 from avg_treatment_2013 a join avg_control_2013 b on (a.month = b.month);

create table causal_effects_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table causal_effects_2013 select month, t_taxi1 - c_taxi1, t_taxi2 - c_taxi2, t_subway1 - c_subway1, t_subway2 - c_subway2 from causal_2013_staging order by month;

drop table zone87_changes;
drop table zone234_changes;
drop table zone114_changes;
drop table zone163_changes;
drop table zone236_changes;
drop table zone239_changes;
drop table zone166_changes;
drop table zone75_changes;

drop table treatment_2013;
drop table avg_treatment_2013;
drop table control_2013;
drop table avg_control_2013;

drop table causal_2013_staging;
