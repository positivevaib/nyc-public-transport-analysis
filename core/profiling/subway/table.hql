create external table subway_manhattan (stationID string, complexID string, gtfsStopID string, line string, remoteUnit string, 
division string, stopName string, daytimeRoutes string, borough string, structure string, latitude string, longitude string, 
northDirection string, southDirection string, ada string, adaNotes string)
row format delimited fields terminated by ','
location '/user/jl11257/subway_manhattan/';

create external table remoteUnit_station (remoteUnit string, station string)
row format delimited fields terminated by ','
location '/user/jl11257/subway_remoteunit_station/';