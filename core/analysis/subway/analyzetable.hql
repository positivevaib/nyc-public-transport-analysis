# create a general table for subway, taxi, bike in order to join all three datasets together
create external table general_data
(year int, month int, day int, hour int, count1 int, count2 int)
row format delimited fields terminated by ','
stored as textfile;

ALTER TABLE general_data CHANGE count1 count1 int COMMENT 'count for subway entires, taxi, bike';
ALTER TABLE general_data CHANGE count2 count2 int COMMENT 'count for subway exits';

# calculate the subway data(entries) by year and month and zone(gridid) in 2012-2019
create table subway_for_join AS
select year, month, gridid as zone, sum(netentries) as monthlycumentries 
from subway_data_net_clean_new where (gridid != 0 and year > 2011 and year < 2020) group by year, month, gridid order by year, month, gridid;