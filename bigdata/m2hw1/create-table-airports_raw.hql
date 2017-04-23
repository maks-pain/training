create external table airports_raw (
	iata string, airport string, city string, state string, country string, lat string, lon string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
location '/user/hive/airports';

ANALYZE TABLE airports_raw COMPUTE STATISTICS;
ANALYZE TABLE airports_raw COMPUTE STATISTICS FOR COLUMNS;