REGISTER	file:/home/hadoop/lib/pig/piggybank.jar
DEFINE	CSVLoader	org.apache.pig.piggybank.storage.CSVLoader;

flights = LOAD 's3://hadoop-input/data.csv' USING CSVLoader() AS (Year,Quarter,Month,DayofMonth,
DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,Origin,OriginCityName,
OriginState,OriginStateFips,OriginStateName,OriginWac,Dest,DestCityName,DestState,DestStateFips,
DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,
DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes:double,ArrDel15,
ArrivalDelayGroups,ArrTimeBlk,Cancelled:int,CancellationCode,Diverted:int,CRSElapsedTime,ActualElapsedTime,AirTime,
Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay);

-- new table (Year, Month, FlightData, Origin, Dest, DepTime, ArrTime, ArrDelayMinutes, Cancelled, Diverted)
flights1 = FOREACH flights GENERATE $0, $2, $5, $11, $17, $24, $35, $37, $41, $43;
flights2 = FOREACH flights GENERATE $0, $2, $5, $11, $17, $24, $35, $37, $41, $43;

-- filters the table except for year and month range
flights1 = FILTER flights1 BY ((Origin == 'ORD') AND (Dest != 'JFK') AND (Year != 'Year') AND (Cancelled != 1) AND (Diverted != 1));
flights2 = FILTER flights2 BY ((Dest == 'JFK') AND (Origin != 'ORD') AND (Year != 'Year') AND (Cancelled != 1) AND (Diverted != 1));

flights1 = FILTER flights1 BY ((Year == 2007 AND Month >= 6) OR (Year == 2008 AND Month <= 5));
flights2 = FILTER flights2 BY ((Year == 2007 AND Month >= 6) OR (Year == 2008 AND Month <= 5));

joined = JOIN flights1 BY (Dest, FlightDate), flights2 BY (Origin, FlightDate);
joined = FILTER joined BY (flights1::ArrTime <= flights2::DepTime);
delay = FOREACH joined GENERATE (flights1::ArrDelayMinutes + flights2::ArrDelayMinutes) AS totalDelay;
grouped = GROUP delay ALL;
avgTime = FOREACH grouped GENERATE COUNT(delay.totalDelay), AVG(delay.totalDelay);
STORE avgTime INTO 's3://hadoop-result-bucket/HW3-pig-filter-first/';