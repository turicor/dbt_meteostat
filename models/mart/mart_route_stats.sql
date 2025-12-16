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



with airport_stats as (
    select origin as airport_origin
        ,dest as airport_destination
        ,count(flight_number) as total_flights
        ,count(distinct tail_number) as total_planes
        ,count(distinct airline) as total_airlines
        ,sum(actual_elapsed_time)/count(*) as average_elapsed_time
        ,sum(arr_delay)/count(*) as average_delay_arrival
        ,max(arr_delay) as max_delay_arrival
        ,min(arr_delay) as min_delay_arrival
        ,count(*) filter (where cancelled =1) as cancelled_flights
        ,count(*) filter (where diverted =1) as diverted_flights
    from {{ref('prep_flights')}}
    group by airport_origin, airport_destination
)
select p.city
    ,p.country
    ,p.name
    ,airport_stats.*
from airport_stats
left join {{ref('prep_airports')}} as p
    on p.faa = airport_origin
left join {{ref('prep_airports')}} as b
    on b.faa = airport_destination
--group by airport_origin, airport_destination, p.city, p.country, p.name THERE IS NO NEED TO GROUP AGAIN!
order by (airport_origin, airport_destination) desc






/*
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
*/