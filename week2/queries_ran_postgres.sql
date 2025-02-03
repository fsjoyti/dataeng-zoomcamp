SELECT table_schema,table_name 
FROM information_schema.tables 
ORDER BY table_schema,table_name;

use DATABASE kestra;

SET search_path TO kestra;

SELECT session_user, current_database();

select * from public.yellow_tripdata limit 20;

select distinct date_trunc('month', tpep_pickup_datetime)::date from public.yellow_tripdata;

truncate table public.yellow_tripdata;

select count(*) from public.yellow_tripdata;

truncate table public.green_tripdata;


select count(*) from public.green_tripdata;
