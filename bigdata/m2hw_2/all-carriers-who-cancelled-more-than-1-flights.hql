SELECT 
	count(*) as num_of_flights,
	code, 
	max(distinct description) as carrier, 
	concat_ws(", ",collect_set(city)) as cities,
	concat_ws(", ",collect_set(CancellationCode)) as CancellationCodes
FROM (select * from flights_raw WHERE cancelled = 1) canceled_flights
JOIN carriers_raw ON uniquecarrier=code
JOIN airports_raw ON origin = airports_raw.iata
GROUP BY code
ORDER BY num_of_flights DESC;