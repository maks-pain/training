create external table carriers_raw (
	code string, 
	description string		
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
location '/user/hive/carriers';

ANALYZE TABLE carriers_raw COMPUTE STATISTICS;
ANALYZE TABLE carriers_raw COMPUTE STATISTICS FOR COLUMNS;