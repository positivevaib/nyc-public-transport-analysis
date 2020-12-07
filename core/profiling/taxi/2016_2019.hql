CREATE TABLE rp3261.clean_data_2016_19
(year int comment 'from 2016 to 2019', month int comment 'from 1 to 12', day int comment 'from 1 to 31', hour int comment 'from 0 to 23', LocationID int);

insert into clean_data_2016_19
select year, month, day, hour, loc
from raw_data_2016_19_final
where year>=2017 and year<=2019
and loc in 
(4,12,13,24,41,42,43,45,48,50,68,74,75,79,87,88,90,100,103,104,
105,107,113,114,116,120,125,127,128,137,140,141,142,143,144,148,
151,152,153,158,161,162,163,164,166,170,186,194,202,209,211,224,
229,230,231,232,233,234,236,237,238,239,243,244,246,249,261,262,263);

insert into clean_data_2016_19
select year, month, day, hour, loc
from raw_data_2016_19_final
where year=2016 and month>=7
and loc in 
(4,12,13,24,41,42,43,45,48,50,68,74,75,79,87,88,90,100,103,104,
105,107,113,114,116,120,125,127,128,137,140,141,142,143,144,148,
151,152,153,158,161,162,163,164,166,170,186,194,202,209,211,224,
229,230,231,232,233,234,236,237,238,239,243,244,246,249,261,262,263);