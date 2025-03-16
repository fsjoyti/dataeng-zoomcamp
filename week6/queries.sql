CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);

select * from public.processed_events;

 CREATE TABLE taxi_events_counts_aggregated (
          PULocationID INTEGER,
            DOLocationID INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_trips BIGINT
           );
   SELECT 
teca.total_trips ,
zpu."Zone" AS "pickup_zone",
zdo."Zone" AS "dropoff_zone"
FROM 
taxi_events_counts_aggregated teca ,
zones zpu,
zones zdo
WHERE
teca.PULocationID = zpu."LocationID" AND
teca.DOLocationID = zdo."LocationID"
order by teca.total_trips desc