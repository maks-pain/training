SELECT count(*) as num_of_flights, code, max(distinct description) as carrier
FROM flights_raw, carriers_raw 
WHERE uniquecarrier=code
GROUP BY code
ORDER BY num_of_flights  DESC LIMIT 1;