select * from green_taxi_data offset ((select count(*) from green_taxi_data)-1);

--Question 3
SELECT
  count(*)
FROM
  green_taxi_data
WHERE
  (lpep_pickup_datetime::date>='2019-10-01' AND
  lpep_pickup_datetime::date<'2019-11-01') and (trip_distance <= 1);
  
  -- 104830
  
  SELECT
  COUNT(1)
FROM
  green_taxi_data
WHERE
  (lpep_pickup_datetime::date>='2019-10-01' AND
  lpep_pickup_datetime::date<'2019-11-01') and trip_distance > 1 and trip_distance <=3;
  
  -- 198995
  
   SELECT
  COUNT(1)
FROM
  green_taxi_data
WHERE
  (lpep_pickup_datetime::date>='2019-10-01' AND
  lpep_pickup_datetime::date <'2019-11-01') and trip_distance > 3 and trip_distance <=7;
  
  --109642
  
    SELECT
  COUNT(1)
FROM
  green_taxi_data
WHERE
  (lpep_pickup_datetime::date>='2019-10-01' AND
  lpep_pickup_datetime::date <'2019-11-01') and trip_distance > 7 and trip_distance <=10;
  --27686
  
  SELECT
  COUNT(1)
FROM
  green_taxi_data
WHERE
  (lpep_pickup_datetime::date>='2019-10-01' AND
  lpep_pickup_datetime::date <'2019-11-01') and trip_distance > 10; 
  --35201
  
  -- Question 3
  
select date_trunc('day', lpep_pickup_datetime) as pickup_day,
  max(trip_distance) as max_trip_distance
from green_taxi_data
group by pickup_day
order by max_trip_distance desc
limit 1;

-- Question 5
select concat(coalesce(puzones."Zone",'Unknown')), 
  sum(total_amount) as total_price_ride
from green_taxi_data as taxi
  left join zones as puzones
    on taxi."PULocationID" = puzones."LocationID"
	where lpep_pickup_datetime::date =  '2019-10-18'
group by 1
order by total_price_ride desc;

-- Question 6
SELECT 
lpep_pickup_datetime,
lpep_dropoff_datetime,
tip_amount,
zpu."Zone" AS "pickup_zone",
zdo."Zone" AS "dropoff_zone"
FROM 
green_taxi_data t,
zones zpu,
zones zdo
WHERE
t."PULocationID" = zpu."LocationID" AND
t."DOLocationID" = zdo."LocationID" AND
zpu."Zone" = 'East Harlem North'
ORDER BY tip_amount DESC;