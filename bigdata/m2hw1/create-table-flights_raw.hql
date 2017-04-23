create external table flights_raw (
	Year	int,
	Month	int,								
	DayofMonth	int,							
	DayOfWeek	int,							
	DepTime	int,							
	CRSDepTime	int,							
	ArrTime	int,								
	CRSArrTime	int,							
	UniqueCarrier	string,						
	FlightNum	int,							
	TailNum	string,								
	ActualElapsedTime	int,					
	CRSElapsedTime	int,						
	AirTime	int,								
	ArrDelay	int,							
	DepDelay	int,							
	Origin	string,								
	Dest	string,								
	Distance	int,							
	TaxiIn	int,								
	TaxiOut	int,								
	Cancelled	int,							
	CancellationCode string,						
	Diverted	int,							
	CarrierDelay	int,						
	WeatherDelay	int,						
	NASDelay	int,							
	SecurityDelay	int,						
	LateAircraftDelay	int							
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
location '/user/hive/flights';

ANALYZE TABLE flights_raw COMPUTE STATISTICS;
ANALYZE TABLE flights_raw COMPUTE STATISTICS  FOR COLUMNS;