/*
In a table `mart_selected_faa_stats_weather.sql` we want to see **for each airport daily**:

- only the airports we collected the weather data for
- unique number of departures connections
- unique number of arrival connections
- how many flight were planned in total (departures & arrivals)
- how many flights were canceled in total (departures & arrivals)
- how many flights were diverted in total (departures & arrivals)
- how many flights actually occured in total (departures & arrivals)
- *(optional) how many unique airplanes travelled on average*
- *(optional) how many unique airlines were in service  on average* 
- (optional) add city, country and name of the airport
- daily min temperature
- daily max temperature
- daily precipitation 
- daily snow fall
- daily average wind direction 
- daily average wind speed
- daily wnd peakgust
*/



-- departures
with departures as (
    select origin as faa
        ,flight_date
        ,count(distinct dest) as destination_connections
        ,count(*) as dep_planned_flights
        ,count(*) filter (where cancelled = 1) as dep_cancelled_flights
        ,count(*) filter (where diverted = 1) as dep_diverted_flights
        ,count(*) filter (where cancelled = 0) as dep_ocurred_flights
        ,count(distinct tail_number) as dep_unique_airplanes
        ,count(distinct airline) as dep_unique_airlines
    from {{ref('prep_flights')}}
    group by faa, flight_date
)
-- arrivals
,arrivals as (
    select dest as faa
        ,flight_date
        ,count(distinct origin) as arrival_connections
        ,count(*) as arr_planned_flights
        ,count(*) filter (where cancelled = 1) as arr_cancelled_flights 
        ,count(*) filter (where diverted = 1) as arr_diverted_flights
        ,count(*) filter (where cancelled = 0) as arr_ocurred_flights
        ,count(distinct tail_number) as arr_unique_airplanes
        ,count(distinct airline) as arr_unique_airlines
    from {{ref('prep_flights')}}
    group by faa, flight_date
)
-- combined
,totals as (
    select faa
    ,flight_date
    ,destination_connections
    ,arrival_connections
    ,(destination_connections + arrival_connections)::NUMERIC/2 as connections
    ,dep_planned_flights + arr_planned_flights as planned_flights
    ,dep_cancelled_flights + arr_cancelled_flights as cancelled_flights
    ,dep_diverted_flights + arr_diverted_flights as diverted_flights
    ,dep_ocurred_flights + arr_ocurred_flights as ocurred_flights
    ,(dep_unique_airplanes + arr_unique_airplanes)::NUMERIC/2 as unique_airplanes
    ,(dep_unique_airlines + arr_unique_airlines)::NUMERIC/2 as unique_airlines
from departures
join arrivals
using (faa, flight_date)
)
-- general call of data
SELECT flight_date
    ,a.city
    ,a.country
    ,a.name
    ,totals.*
    ,w.min_temp_c
    ,w.max_temp_c
    ,w.precipitation_mm
    ,w.max_snow_mm
    ,w.avg_wind_direction
    ,w.avg_wind_speed_kmh
    ,w.wind_peakgust_kmh
from totals
left join {{ref('prep_airports')}} a
    using (faa)
right join {{ref('prep_weather_daily')}} w 
    on w.airport_code = totals.faa
