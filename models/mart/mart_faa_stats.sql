/*#### 5.1 Airport Stats

In a table `mart_faa_stats.sql` we want to see **for each airport over all time**:

- unique number of departures connections

- unique number of arrival connections

- how many flight were planned in total (departures & arrivals)

- how many flights were canceled in total (departures & arrivals)

- how many flights were diverted in total (departures & arrivals)

- how many flights actually occured in total (departures & arrivals)

- *(optional) how many unique airplanes travelled on average*

- *(optional) how many unique airlines were in service  on average* 

- add city, country and name of the airport
*/

select 
	origine.faa
	,origine.name
	,origine.city
	,origine.country
	,count (distinct pflights.origin) as unique_departures
	,count (distinct dest) as unique_destinations
	,count (*) as planned_flights
	,count (*) FILTER (where cancelled = 1) as cancelled_flights
	,count (*) FILTER (where diverted = 1) as diverted_flights
	,COUNT(*) - COUNT(*) FILTER (WHERE cancelled = 1) AS occurred_flights
	,count (DISTINCT tail_number) FILTER (WHERE cancelled != 1) as unique_airplanes
	,count (distinct airline) FILTER (WHERE cancelled != 1) AS unique_airlines 
FROM prep_flights as pflights
INNER JOIN prep_airports AS destine
    ON destine.faa = pflights.dest
inner join prep_airports AS origine
	on origine.faa = pflights.origin
group by origine.faa, origine.name, origine.city, origine.country
;