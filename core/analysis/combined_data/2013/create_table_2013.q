create table causal_2013 (year int, month int, zone int, taxi bigint, subway bigint);
insert overwrite table causal_2013 select year, month, zone, taxi, subway from all_three where (((year = 2012 and month > 6) or year = 2013 or year = 2014 or (year = 2015 and month <= 6)) and (zone = 87 or zone = 114 or zone = 234 or zone = 163 or zone = 236 or zone = 239 or zone = 166 or zone = 75)); 
