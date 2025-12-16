/* In a table `mart_weather_weekly.sql` we want to see **all** weather stats from the `prep_weather_daily` model aggregated weekly. 

- consider whether the metric should be Average, Maximum, Minimum, Sum or [Mode](https://wiki.postgresql.org/wiki/Aggregate_Mode)
*/


WITH daily_data AS (
        SELECT * 
        FROM {{ref('prep_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		,DATE_PART('week', date) AS week_number			-- number of the week of year
        FROM daily_data 
    )
    SELECT airport_code
        ,week_number
        ,avg(avg_temp_c)::numeric(3,1) as media_temperatura
        ,min(min_temp_c)::numeric(3,1) as minima_temperatura
        ,max(max_temp_c) as masima_temperatura
        ,sum(precipitation_mm)::numeric(3,1) as precipitacion
        ,sum(max_snow_mm)::numeric(3,1)  as ñeve
        ,avg(avg_wind_direction) as direicion_vientu
        ,avg(avg_wind_speed_kmh)::numeric(5,2) as velocidá_vientu
        ,avg(wind_peakgust_kmh)::numeric(5,2) as rabasera
        ,avg(avg_pressure_hpa)::numeric(6,2) as presión
        ,sum(sun_minutes)::numeric(6,1) as soleyada
    FROM add_features
    group by airport_code, week_number
    order by week_number asc