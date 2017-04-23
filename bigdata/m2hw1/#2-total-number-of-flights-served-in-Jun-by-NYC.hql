select sum(grouped_flights.num_of_flights) as total_flights, city 
from ( 
  	select 
		count(*) as num_of_flights, 
		max(Origin) as origin_dest
	from flights_raw 
	where month = 6
	group by Origin
  UNION
  	select 
		count(*) as num_of_flights, 
		max(Dest) as origin_dest
	from flights_raw 
	where month = 6
	group by  Dest
) grouped_flights
JOIN airports_raw ON grouped_flights.origin_dest = airports_raw.iata
WHERE city = 'New York'
GROUP BY city;