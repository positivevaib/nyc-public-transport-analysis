create table treatment_2013 (month int, zone87_taxi1 double, zone87_taxi2 double, zone87_subway1 double, zone87_subway2 double, zone234_taxi1 double, zone234_taxi2 double, zone234_subway1 double, zone234_subway2 double);
insert overwrite table treatment_2013 select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2 from zone87_changes a join zone234_changes b on (a.month = b.month);

create table avg_treatment_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table avg_treatment_2013 select month, (zone87_taxi1 + zone234_taxi1)/2, (zone87_taxi2 + zone234_taxi2)/2, (zone87_subway1 + zone234_subway1)/2, (zone87_subway2 + zone234_subway2)/2 from treatment_2013;

create table control_2013 (month int, zone236_taxi1 double, zone236_taxi2 double, zone236_subway1 double, zone236_subway2 double, zone239_taxi1 double, zone239_taxi2 double, zone239_subway1 double, zone239_subway2 double);
insert overwrite table control_2013 select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2 from zone236_changes a join zone239_changes b on (a.month = b.month);

create table avg_control_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table avg_control_2013 select month, (zone236_taxi1 + zone239_taxi1)/2, (zone236_taxi2 + zone239_taxi2)/2, (zone236_subway1 + zone239_subway1)/2, (zone236_subway2 + zone239_subway2)/2 from control_2013;

create table causal_2013_staging (month int, t_taxi1 double, t_taxi2 double, t_subway1 double, t_subway2 double, c_taxi1 double, c_taxi2 double, c_subway1 double, c_subway2 double);
insert overwrite table causal_2013_staging select a.month, a.taxi1, a.taxi2, a.subway1, a.subway2, b.taxi1, b.taxi2, b.subway1, b.subway2 from avg_treatment_2013 a join avg_control_2013 b on (a.month = b.month);

create table causal_effects_2013 (month int, taxi1 double, taxi2 double, subway1 double, subway2 double);
insert overwrite table causal_effects_2013 select month, t_taxi1 - c_taxi1, t_taxi2 - c_taxi2, t_subway1 - c_subway1, t_subway2 - c_subway2 from causal_2013_staging order by month;
