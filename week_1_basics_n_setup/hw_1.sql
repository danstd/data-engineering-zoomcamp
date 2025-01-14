-- Question 3
-- How many taxi trips were totally made on January 15?

select count(distinct green_taxi_trip.index)
from green_taxi_trip
where to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-15'
and to_char(lpep_dropoff_datetime, 'YYYY-MM-DD') = '2019-01-15';

-- Question 4
-- Which was the day with the largest trip distance
-- Use the pick up time for your calculations.

select to_char(gtt.lpep_pickup_datetime, 'YYYY-MM-DD')
from green_taxi_trip gtt
inner join
	(select max(trip_distance) as max_trip
	 from green_taxi_trip) m_trip
on gtt.trip_distance = m_trip.max_trip;

-- Question 5. The number of passengers
-- In 2019-01-01 how many trips had 2 and 3 passengers?

select passenger_count, count(passenger_count)
from (
  select passenger_count from green_taxi_trip
  where(to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-01'
  or to_char(lpep_dropoff_datetime, 'YYYY-MM-DD') = '2019-01-01') )
  p_data
where passenger_count in (2,3)
group by passenger_count;

-- Question 6. Largest tip
-- For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
-- We want the name of the zone, not the id.

select distinct zone_tip.zone
from (
	select tz.locationid, tz.zone, gtt.*,
	dense_rank() over(order by tip_amount desc) as tip_rank
	from green_taxi_trip gtt
	inner join taxi_zone tz
	on gtt."DOLocationID" = tz.locationid
	where gtt."PULocationID" in
	(select locationid from taxi_zone tz_in where tz_in.zone = 'Astoria'))
	zone_tip
where tip_rank = 1;