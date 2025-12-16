/*
In a table `mart_route_stats.sql` we want to see **for each route over all time**:

- origin airport code
- destination airport code 
- total flights on this route
- unique airplanes
- unique airlines
- on average what is the actual elapsed time
- on average what is the delay on arrival
- what was the max delay?
- what was the min delay?
- total number of cancelled 
- total number of diverted
- add city, country and name for both, origin and destination, airports
*/


select 
	origine.faa AS faa_origin
	,origine.name
	,origine.city
	,origine.country
	,destine.faa AS faa_destination
	,destine.name
	,destine.city
	,destine.country
	,count(*) AS total_route_flights 
	,count (distinct tail_number) as unique_airplanes
	,count (distinct airline) AS unique_airlines 
	,avg (actual_elapsed_time) as average_elapsed_time
	,avg (arr_delay) FILTER (WHERE arr_delay > 0) AS average_delay_arrival
	,max(arr_delay) FILTER (WHERE arr_delay > 0) AS max_delay_arrival
	,min(arr_delay) FILTER (WHERE arr_delay > 0) AS min_delay_arrival
	,count (*) FILTER (where cancelled = 1) as cancelled_flights
	,count (*) FILTER (where diverted = 1) as diverted_flights
FROM prep_flights as pflights
INNER JOIN prep_airports AS destine
    ON destine.faa = pflights.dest
inner join prep_airports AS origine
	on origine.faa = pflights.origin
group BY origine.faa
	,origine.name
	,origine.city
	,origine.country
	,destine.faa
	,destine.name
	,destine.city
	,destine.country
;