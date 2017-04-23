set hive.cbc.enable=true; 
set hive.compute.query.using.stats=true; 
set hive.stats.fetch.column.stats=true; 
set hive.stats.fetch.partition.stats=true;

SELECT count(*) as num_of_flights, code, max(distinct description) as carrier
FROM flights_raw, carriers_raw 
WHERE uniquecarrier=code
GROUP BY code;