SELECT sum(grouped_flights.num_of_flights) as total_flights, ap_code, max(airport) as airport_name
FROM ( 
  	select 
		count(*) as num_of_flights, 
		max(Origin) as ap_code
	from flights_raw 
	where month >= 6 and month <= 8 
	group by Origin
  UNION
  	select 
		count(*) as num_of_flights, 
		max(Dest) as ap_code
	from flights_raw 
	where month >= 6 and month <= 8
	group by  Dest
) grouped_flights
JOIN airports_raw ON grouped_flights.ap_code = airports_raw.iata
WHERE airports_raw.country = 'USA'
GROUP BY grouped_flights.ap_code
ORDER BY total_flights DESC
LIMIT 5;